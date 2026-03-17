-- AdvancedSystems.lua
-- Single-file, modular systems for Roblox servers
-- Includes: EventBus, DataSerializer, LRU CacheManager, PlayerDataManager (with retries),
-- InventorySystem, LeaderboardManager, AntiCheat, and bootstrap.

-- ============================================
-- Module: EventBus (Observer pattern, safe firing)
-- ============================================
local EventBus = {}
EventBus.__index = EventBus

function EventBus.new()
    local self = setmetatable({}, EventBus)
    self._listeners = {}            -- eventName -> { id -> callback }
    self._nextListenerId = 0
    self._listenerCount = 0
    return self
end

function EventBus:Connect(eventName, callback)
    assert(type(eventName) == "string", "eventName must be string")
    assert(type(callback) == "function", "callback must be function")
    self._listeners[eventName] = self._listeners[eventName] or {}
    self._nextListenerId = self._nextListenerId + 1
    local id = tostring(self._nextListenerId)
    self._listeners[eventName][id] = callback
    self._listenerCount = self._listenerCount + 1

    local disconnected = false
    return {
        Disconnect = function()
            if disconnected then return end
            disconnected = true
            if self._listeners[eventName] then
                if self._listeners[eventName][id] then
                    self._listeners[eventName][id] = nil
                    self._listenerCount = math.max(0, self._listenerCount - 1)
                end
                if next(self._listeners[eventName]) == nil then
                    self._listeners[eventName] = nil
                end
            end
        end
    }
end

function EventBus:Once(eventName, callback)
    local conn
    conn = self:Connect(eventName, function(...)
        if conn and conn.Disconnect then
            conn.Disconnect()
        end
        callback(...)
    end)
    return conn
end

-- Fire will run *all listeners* inside a single spawned worker so we don't spawn N threads.
function EventBus:Fire(eventName, ...)
    local listeners = self._listeners[eventName]
    if not listeners then return end
    -- copy references to avoid mutation issues while iterating
    local copy = {}
    for id, cb in pairs(listeners) do
        if type(cb) == "function" then
            copy[#copy+1] = cb
        end
    end
    if #copy == 0 then return end

    -- Run callbacks sequentially inside one task so they won't spawn dozens of threads.
    task.spawn(function()
        for i = 1, #copy do
            local ok, err = pcall(copy[i], ...)
            if not ok then
                warn(string.format("EventBus listener error on '%s': %s", eventName, tostring(err)))
            end
        end
    end)
end

function EventBus:GetListenerCount()
    return self._listenerCount
end

-- ============================================
-- Module: DataSerializer (schemas, transforms, validators)
-- ============================================
local DataSerializer = {}
DataSerializer.__index = DataSerializer

function DataSerializer.new()
    local self = setmetatable({}, DataSerializer)
    self._schemas = {} -- dataType -> {defaults, validators, transformers}
    return self
end

-- schema = { defaults = {...}, validators = { key = fn }, transformers = { key = {serialize = fn, deserialize = fn} } }
function DataSerializer:RegisterSchema(dataType, schema)
    assert(type(dataType) == "string", "dataType must be string")
    schema = schema or {}
    self._schemas[dataType] = {
        defaults = schema.defaults or {},
        validators = schema.validators or {},
        transformers = schema.transformers or {}
    }
end

function DataSerializer:Serialize(data, dataType)
    if not data or type(data) ~= "table" then return data end
    local schema = self._schemas[dataType]
    if not schema then return data end
    local out = {}
    for key, default in pairs(schema.defaults) do
        local value = data[key]
        if value == nil then
            out[key] = default
        else
            local t = schema.transformers[key]
            if t and type(t.serialize) == "function" then
                local ok, res = pcall(t.serialize, value)
                out[key] = ok and res or default
            else
                out[key] = value
            end
        end
    end
    -- also include extra keys not in defaults (optional)
    for key, value in pairs(data) do
        if out[key] == nil then
            local t = schema.transformers[key]
            if t and type(t.serialize) == "function" then
                local ok, res = pcall(t.serialize, value)
                out[key] = ok and res or value
            else
                out[key] = value
            end
        end
    end
    return out
end

function DataSerializer:Deserialize(data, dataType)
    if not data or type(data) ~= "table" then return data end
    local schema = self._schemas[dataType]
    if not schema then return data end
    local out = {}
    for key, defaultValue in pairs(schema.defaults) do
        local v = data[key]
        if v ~= nil then
            local t = schema.transformers[key]
            if t and type(t.deserialize) == "function" then
                local ok, res = pcall(t.deserialize, v)
                out[key] = ok and res or defaultValue
            else
                out[key] = v
            end
        else
            out[key] = defaultValue
        end
    end
    -- include additional keys present in data
    for key, v in pairs(data) do
        if out[key] == nil then
            local t = schema.transformers[key]
            if t and type(t.deserialize) == "function" then
                local ok, res = pcall(t.deserialize, v)
                out[key] = ok and res or v
            else
                out[key] = v
            end
        end
    end
    return out
end

function DataSerializer:Validate(data, dataType)
    local schema = self._schemas[dataType]
    if not schema then return true end
    for key, validator in pairs(schema.validators) do
        if data[key] ~= nil and type(validator) == "function" then
            local ok = pcall(validator, data[key])
            if not ok or not validator(data[key]) then
                return false, tostring(key) .. " failed validation"
            end
        end
    end
    return true
end

-- ============================================
-- Module: CacheManager (O(1) LRU using hashmap + doubly-linked list)
-- ============================================
local CacheManager = {}
CacheManager.__index = CacheManager

function CacheManager.new(maxSize)
    local self = setmetatable({}, CacheManager)
    self._cache = {}          -- key -> node { key, value, expiration, prev, next }
    self._maxSize = math.max(1, tonumber(maxSize) or 100)
    self._size = 0
    -- doubly-linked list head/tail for LRU (head = most recent, tail = least recent)
    self._head = nil
    self._tail = nil
    self._hits = 0
    self._misses = 0
    return self
end

local function attachToHead(self, node)
    node.prev = nil
    node.next = self._head
    if self._head then self._head.prev = node end
    self._head = node
    if not self._tail then self._tail = node end
end

local function removeNode(self, node)
    if not node then return end
    if node.prev then
        node.prev.next = node.next
    else
        self._head = node.next
    end
    if node.next then
        node.next.prev = node.prev
    else
        self._tail = node.prev
    end
    node.prev = nil
    node.next = nil
end

function CacheManager:Get(key)
    local node = self._cache[key]
    if not node then
        self._misses = self._misses + 1
        return nil
    end
    -- expire?
    if node.expiration and tick() > node.expiration then
        self:Remove(key)
        self._misses = self._misses + 1
        return nil
    end
    -- hit: move to head
    removeNode(self, node)
    attachToHead(self, node)
    self._hits = self._hits + 1
    return node.value
end

function CacheManager:Set(key, value, ttl)
    local node = self._cache[key]
    local expiration = ttl and (tick() + ttl) or nil
    if node then
        -- update existing
        node.value = value
        node.expiration = expiration
        removeNode(self, node)
        attachToHead(self, node)
        return
    end
    -- new node
    if self._size >= self._maxSize then
        -- evict tail (LRU)
        if self._tail then
            self:Remove(self._tail.key)
        end
    end
    local newNode = { key = key, value = value, expiration = expiration, prev = nil, next = nil }
    self._cache[key] = newNode
    attachToHead(self, newNode)
    self._size = self._size + 1
end

function CacheManager:Remove(key)
    local node = self._cache[key]
    if not node then return end
    removeNode(self, node)
    self._cache[key] = nil
    self._size = math.max(0, self._size - 1)
end

function CacheManager:EvictLRU()
    if not self._tail then return end
    self:Remove(self._tail.key)
end

function CacheManager:CleanupExpired()
    local now = tick()
    -- iterate keys (not ideal but expiration cleanup is occasional)
    for k, node in pairs(self._cache) do
        if node.expiration and now > node.expiration then
            self:Remove(k)
        end
    end
end

function CacheManager:GetStats()
    local total = self._hits + self._misses
    local hitRate = total > 0 and (self._hits / total) or 0
    return {
        size = self._size,
        maxSize = self._maxSize,
        hitRate = hitRate,
        hits = self._hits,
        misses = self._misses
    }
end

-- ============================================
-- Module: PlayerDataManager (DataStore wrapper with retries, caching, versioning)
-- ============================================
local PlayersService = game:GetService("Players")
local DataStoreService = game:GetService("DataStoreService")
local RunService = game:GetService("RunService")
local HttpService = game:GetService("HttpService")

local PlayerDataManager = {}
PlayerDataManager.__index = PlayerDataManager

-- helper: robust pcall with retries and exponential backoff
local function withRetries(fn, attempts, baseDelay)
    attempts = attempts or 3
    baseDelay = baseDelay or 0.5
    local lastErr
    for i = 1, attempts do
        local ok, res = pcall(fn)
        if ok then
            return true, res
        else
            lastErr = res
            local waitTime = baseDelay * (2 ^ (i - 1))
            -- jitter small
            wait(waitTime + math.random() * 0.1)
        end
    end
    return false, lastErr
end

function PlayerDataManager.new(dataStoreName, eventBus, options)
    assert(type(dataStoreName) == "string", "dataStoreName must be string")
    local self = setmetatable({}, PlayerDataManager)
    self._dataStore = DataStoreService:GetDataStore(dataStoreName)
    self._eventBus = eventBus
    self._playerData = {}      -- userId string -> data table
    self._serializer = DataSerializer.new()
    self._cache = CacheManager.new((options and options.cacheSize) or 50)
    self._autoSaveInterval = (options and options.autoSaveInterval) or 120
    self._dataVersion = (options and options.dataVersion) or 2
    self._shutdown = false
    self:_registerSchemas()
    self:_setupAutoSave()
    return self
end

function PlayerDataManager:_registerSchemas()
    self._serializer:RegisterSchema("playerData", {
        defaults = {
            coins = 0,
            experience = 0,
            level = 1,
            inventory = {},
            settings = {music = true, sfx = true},
            lastLogin = 0,
            playTime = 0
        },
        validators = {
            coins = function(v) return type(v) == "number" and v >= 0 end,
            level = function(v) return type(v) == "number" and v >= 1 and v <= 999 end
        },
        transformers = {
            inventory = {
                serialize = function(inv) return HttpService:JSONEncode(inv) end,
                deserialize = function(inv) return HttpService:JSONDecode(inv) end
            }
        }
    })
end

function PlayerDataManager:_setupAutoSave()
    -- run in background; safe guard on shutdown
    task.spawn(function()
        while not self._shutdown do
            local interval = self._autoSaveInterval
            -- shorter sleep if shutdown flag set
            for i = 1, math.max(1, interval) do
                if self._shutdown then break end
                wait(1)
            end
            if self._shutdown then break end
            self:SaveAllData()
            -- cleanup cache expirations occasionally
            self._cache:CleanupExpired()
        end
    end)
end

function PlayerDataManager:_migrateData(oldData)
    local migrated = {
        version = self._dataVersion,
        data = oldData.data or oldData
    }
    if not oldData.version then
        migrated.data.settings = migrated.data.settings or {music = true, sfx = true}
    end
    return migrated
end

function PlayerDataManager:LoadPlayerData(player)
    local userId = tostring(player.UserId)
    -- try cache first
    local cached = self._cache:Get(userId)
    if cached then
        self._playerData[userId] = cached
        self._eventBus:Fire("DataLoaded", player, cached)
        return cached
    end

    local success, result = withRetries(function()
        return self._dataStore:GetAsync(userId)
    end, 4, 0.6)

    if not success then
        self._eventBus:Fire("DataLoadFailed", player, result)
        warn("Failed to load data for userId " .. userId .. ": " .. tostring(result))
        return nil
    end

    local raw = result
    if raw then
        if raw.version and raw.version ~= self._dataVersion then
            raw = self:_migrateData(raw)
        end
        local deserialized = self._serializer:Deserialize((raw.data or raw), "playerData")
        -- validate
        local ok, err = self._serializer:Validate(deserialized, "playerData")
        if not ok then
            warn("Validation failed for player data of " .. userId .. ": " .. tostring(err))
            deserialized = self._serializer:Deserialize({}, "playerData") -- fallback
        end
        self._playerData[userId] = deserialized
        self._cache:Set(userId, deserialized, 300)
        self._eventBus:Fire("DataLoaded", player, deserialized)
        return deserialized
    else
        -- new player
        local newData = self._serializer:Deserialize({}, "playerData")
        self._playerData[userId] = newData
        self._eventBus:Fire("NewPlayer", player, newData)
        return newData
    end
end

function PlayerDataManager:SavePlayerData(player)
    local userId = tostring(player.UserId)
    local data = self._playerData[userId]
    if not data then return false, "no data" end
    local serialized = {
        version = self._dataVersion,
        data = self._serializer:Serialize(data, "playerData"),
        timestamp = tick()
    }
    local ok, err = withRetries(function()
        return self._dataStore:SetAsync(userId, serialized)
    end, 4, 0.6)
    if ok then
        self._eventBus:Fire("DataSaved", player, data)
        return true
    else
        self._eventBus:Fire("DataSaveFailed", player, err)
        warn("Failed to save data for " .. userId .. ": " .. tostring(err))
        return false, err
    end
end

function PlayerDataManager:SaveAllData()
    for userId, _ in pairs(self._playerData) do
        local player = PlayersService:GetPlayerByUserId(tonumber(userId))
        if player then
            self:SavePlayerData(player)
        else
            -- Attempt to save offline data as well, but be safer: use pcall with userId
            local data = self._playerData[userId]
            if data then
                local serialized = {
                    version = self._dataVersion,
                    data = self._serializer:Serialize(data, "playerData"),
                    timestamp = tick()
                }
                local ok, err = withRetries(function()
                    return self._dataStore:SetAsync(userId, serialized)
                end, 3, 0.6)
                if not ok then
                    warn("Failed to save offline data for " .. userId .. ": " .. tostring(err))
                end
            end
        end
    end
end

function PlayerDataManager:GetData(player)
    local userId = tostring(player.UserId)
    return self._playerData[userId] or self._cache:Get(userId)
end

function PlayerDataManager:UpdateData(player, key, value)
    local userId = tostring(player.UserId)
    local data = self._playerData[userId]
    if not data then return false end
    local oldValue = data[key]
    data[key] = value
    -- server-side validation if schema has validator
    local schema = self._serializer._schemas["playerData"]
    if schema and schema.validators[key] then
        local ok, err = pcall(schema.validators[key], value)
        if not ok or not schema.validators[key](value) then
            -- revert on invalid
            data[key] = oldValue
            return false, "validation_failed"
        end
    end
    self._cache:Set(userId, data, 300)
    self._eventBus:Fire("DataChanged", player, key, value, oldValue)
    return true
end

function PlayerDataManager:IncrementData(player, key, amount)
    local data = self:GetData(player)
    if data and type(data[key]) == "number" then
        local newValue = data[key] + amount
        local ok, err = self:UpdateData(player, key, newValue)
        if ok then return newValue end
        return nil, err
    end
    return nil, "invalid_key_or_type"
end

function PlayerDataManager:RemovePlayer(player)
    local userId = tostring(player.UserId)
    -- save synchronously with retries
    self:SavePlayerData(player)
    self._playerData[userId] = nil
    self._cache:Remove(userId)
    self._eventBus:Fire("PlayerRemoved", player)
end

function PlayerDataManager:Shutdown()
    -- signal shutdown and save all
    self._shutdown = true
    self:SaveAllData()
end

-- ============================================
-- Module: InventorySystem (server-authoritative inventory)
-- ============================================
local InventorySystem = {}
InventorySystem.__index = InventorySystem

function InventorySystem.new(dataManager, eventBus)
    local self = setmetatable({}, InventorySystem)
    self._dataManager = assert(dataManager)
    self._eventBus = assert(eventBus)
    self._itemDatabase = {}
    self._equippedItems = {} -- userId -> { itemId = true }
    self:_loadItemDatabase()
    self:_setupEventListeners()
    return self
end

function InventorySystem:_loadItemDatabase()
    self._itemDatabase = {
        ["sword_iron"] = {name = "Iron Sword", type = "weapon", maxStack = 1, damage = 15, rarity = "common"},
        ["potion_health"] = {name = "Health Potion", type = "consumable", maxStack = 10, healAmount = 25, rarity = "common"},
        ["coin_gold"] = {name = "Gold Coin", type = "currency", maxStack = 999, value = 1, rarity = "common"},
        ["gem_ruby"] = {name = "Ruby", type = "material", maxStack = 50, value = 100, rarity = "rare"},
        ["armor_dragon"] = {name = "Dragon Armor", type = "armor", maxStack = 1, defense = 50, rarity = "legendary"}
    }
end

function InventorySystem:_setupEventListeners()
    self._eventBus:Connect("DataLoaded", function(player, data)
        self._equippedItems[tostring(player.UserId)] = {}
        self:ValidateInventory(player)
    end)
end

function InventorySystem:ValidateInventory(player)
    local data = self._dataManager:GetData(player)
    if not data or type(data.inventory) ~= "table" then return end
    local validInventory = {}
    for itemId, itemData in pairs(data.inventory) do
        if self._itemDatabase[itemId] then
            -- ensure quantity is number and within bounds
            local qty = tonumber(itemData.quantity) or 0
            local max = self._itemDatabase[itemId].maxStack or 1
            qty = math.clamp(qty, 0, max)
            validInventory[itemId] = {
                quantity = qty,
                acquired = itemData.acquired or tick(),
                equipped = itemData.equipped and true or false
            }
        end
    end
    self._dataManager:UpdateData(player, "inventory", validInventory)
end

function InventorySystem:AddItem(player, itemId, quantity)
    quantity = math.max(1, tonumber(quantity) or 1)
    local itemDef = self._itemDatabase[itemId]
    if not itemDef then return false, "invalid_item" end
    local data = self._dataManager:GetData(player)
    if not data then return false, "no_player_data" end
    data.inventory = data.inventory or {}
    local currentQty = (data.inventory[itemId] and data.inventory[itemId].quantity) or 0
    local newQty = math.min(currentQty + quantity, itemDef.maxStack or 1)
    local added = newQty - currentQty
    data.inventory[itemId] = {
        quantity = newQty,
        acquired = data.inventory[itemId] and data.inventory[itemId].acquired or tick(),
        equipped = data.inventory[itemId] and data.inventory[itemId].equipped or false
    }
    self._dataManager:UpdateData(player, "inventory", data.inventory)
    self._eventBus:Fire("ItemAdded", player, itemId, added, newQty)
    return true, added
end

function InventorySystem:RemoveItem(player, itemId, quantity)
    quantity = math.max(1, tonumber(quantity) or 1)
    local data = self._data_manager and self._data_manager:GetData(player) or self._dataManager:GetData(player)
    data = data or self._dataManager:GetData(player)
    if not data or not data.inventory or not data.inventory[itemId] then
        return false, "item_not_found"
    end
    local currentQty = data.inventory[itemId].quantity or 0
    if currentQty < quantity then
        return false, "insufficient_quantity"
    end
    local newQty = currentQty - quantity
    if newQty <= 0 then
        data.inventory[itemId] = nil
        self:UnequipItem(player, itemId)
    else
        data.inventory[itemId].quantity = newQty
    end
    self._dataManager:UpdateData(player, "inventory", data.inventory)
    self._eventBus:Fire("ItemRemoved", player, itemId, quantity, newQty)
    return true, newQty
end

function InventorySystem:EquipItem(player, itemId)
    local data = self._dataManager:GetData(player)
    if not data or not data.inventory or not data.inventory[itemId] then
        return false, "not_in_inventory"
    end
    local itemDef = self._itemDatabase[itemId]
    if itemDef.type ~= "weapon" and itemDef.type ~= "armor" then
        return false, "not_equippable"
    end
    local userId = tostring(player.UserId)
    self._equippedItems[userId] = self._equippedItems[userId] or {}
    -- ensure only one per type
    for equippedId, _ in pairs(self._equippedItems[userId]) do
        if self._itemDatabase[equippedId] and self._itemDatabase[equippedId].type == itemDef.type then
            self:UnequipItem(player, equippedId)
        end
    end
    data.inventory[itemId].equipped = true
    self._equippedItems[userId][itemId] = true
    self._dataManager:UpdateData(player, "inventory", data.inventory)
    self._eventBus:Fire("ItemEquipped", player, itemId)
    return true
end

function InventorySystem:UnequipItem(player, itemId)
    local data = self._dataManager:GetData(player)
    if data and data.inventory and data.inventory[itemId] then
        data.inventory[itemId].equipped = false
        self._dataManager:UpdateData(player, "inventory", data.inventory)
    end
    local userId = tostring(player.UserId)
    if self._equippedItems[userId] then
        self._equippedItems[userId][itemId] = nil
    end
    self._eventBus:Fire("ItemUnequipped", player, itemId)
    return true
end

function InventorySystem:HasItem(player, itemId, quantity)
    quantity = math.max(1, tonumber(quantity) or 1)
    local data = self._dataManager:GetData(player)
    if not data or not data.inventory then return false end
    local item = data.inventory[itemId]
    return item and (item.quantity or 0) >= quantity
end

function InventorySystem:GetItemCount(player, itemId)
    local data = self._dataManager:GetData(player)
    if not data or not data.inventory then return 0 end
    return (data.inventory[itemId] and data.inventory[itemId].quantity) or 0
end

function InventorySystem:TradeItem(fromPlayer, toPlayer, itemId, quantity)
    quantity = math.max(1, tonumber(quantity) or 1)
    -- server-side authoritative trade: remove then add; rollback on fail
    if not self:HasItem(fromPlayer, itemId, quantity) then
        return false, "insufficient_items"
    end
    local ok, res = self:RemoveItem(fromPlayer, itemId, quantity)
    if not ok then return false, res end
    local ok2, added = self:AddItem(toPlayer, itemId, quantity)
    if not ok2 then
        -- rollback
        self:AddItem(fromPlayer, itemId, quantity)
        return false, "failed_to_add_to_recipient"
    end
    self._eventBus:Fire("ItemTraded", fromPlayer, toPlayer, itemId, quantity)
    return true
end

function InventorySystem:CraftItem(player, recipeId)
    -- very small recipe example; extend as needed
    local recipes = {
        ["sword_steel"] = {requirements = {["sword_iron"] = 1, ["gem_ruby"] = 2}, result = "sword_steel", quantity = 1}
    }
    local recipe = recipes[recipeId]
    if not recipe then return false, "invalid_recipe" end
    for reqItem, reqQty in pairs(recipe.requirements) do
        if not self:HasItem(player, reqItem, reqQty) then
            return false, "missing_material_" .. reqItem
        end
    end
    -- remove requirements
    for reqItem, reqQty in pairs(recipe.requirements) do
        self:RemoveItem(player, reqItem, reqQty)
    end
    self:AddItem(player, recipe.result, recipe.quantity)
    self._eventBus:Fire("ItemCrafted", player, recipeId, recipe.result)
    return true
end

-- ============================================
-- Module: LeaderboardManager (periodic, efficient updates)
-- ============================================
local LeaderboardManager = {}
LeaderboardManager.__index = LeaderboardManager

function LeaderboardManager.new(dataManager)
    local self = setmetatable({}, LeaderboardManager)
    self._dataManager = assert(dataManager)
    self._leaderboards = {} -- name -> { folder, statKey, stats }
    self._updateInterval = 5
    self:_setupUpdateLoop()
    return self
end

function LeaderboardManager:CreateLeaderboard(name, statKey, parent)
    local folderParent = parent or workspace
    -- prefer to use a dedicated Leaderstats folder under Players, fallback safe
    local folder = Instance.new("Folder")
    folder.Name = name
    folder.Parent = folderParent
    self._leaderboards[name] = {
        folder = folder,
        statKey = statKey,
        stats = {}
    }
    return folder
end

function LeaderboardManager:UpdatePlayerStat(player, leaderboardName, value)
    local lb = self._leaderboards[leaderboardName]
    if not lb then return end
    local userId = tostring(player.UserId)
    if not lb.stats[userId] then
        local stat = Instance.new("IntValue")
        stat.Name = lb.statKey
        stat.Parent = lb.folder
        lb.stats[userId] = stat
    end
    lb.stats[userId].Value = tonumber(value) or 0
end

function LeaderboardManager:_setupUpdateLoop()
    task.spawn(function()
        while true do
            wait(self._updateInterval)
            for _, player in ipairs(PlayersService:GetPlayers()) do
                local data = self._dataManager:GetData(player)
                if data then
                    for name, lb in pairs(self._leaderboards) do
                        if data[lb.statKey] ~= nil then
                            self:UpdatePlayerStat(player, name, data[lb.statKey])
                        end
                    end
                end
            end
        end
    end)
end

-- ============================================
-- Module: AntiCheat (basic server-side checks & alerts)
-- ============================================
local AntiCheat = {}
AntiCheat.__index = AntiCheat

function AntiCheat.new(eventBus, options)
    local self = setmetatable({}, AntiCheat)
    self._eventBus = assert(eventBus)
    options = options or {}
    self._suspiciousPlayers = {}
    self._thresholds = {
        maxCoinsPerMinute = options.maxCoinsPerMinute or 10000,
        maxItemsPerMinute = options.maxItemsPerMinute or 50,
        maxLevelDifference = options.maxLevelDifference or 10
    }
    self._playerLogs = {} -- userId -> { changes = { ... }, startTime }
    self:_setupMonitoring()
    return self
end

function AntiCheat:_setupMonitoring()
    self._eventBus:Connect("DataChanged", function(player, key, newValue, oldValue)
        -- server-side only
        self:ValidateChange(player, key, newValue, oldValue)
    end)
    self._eventBus:Connect("ItemAdded", function(player, itemId, addedQty)
        self:LogTransaction(player, "add", itemId, addedQty)
    end)
end

function AntiCheat:ValidateChange(player, key, newValue, oldValue)
    local userId = tostring(player.UserId)
    self._playerLogs[userId] = self._playerLogs[userId] or { changes = {}, startTime = tick() }
    table.insert(self._playerLogs[userId].changes, {
        key = key,
        time = tick(),
        delta = (type(newValue) == "number" and type(oldValue) == "number") and (newValue - oldValue) or 0
    })
    -- coins per minute check
    if key == "coins" then
        local now = tick()
        local recent = 0
        for i = #self._playerLogs[userId].changes, 1, -1 do
            local c = self._playerLogs[userId].changes[i]
            if c.key == "coins" and (now - c.time) <= 60 then
                recent = recent + (c.delta or 0)
            end
        end
        if recent > self._thresholds.maxCoinsPerMinute then
            self:FlagPlayer(player, "coin_generation", recent)
        end
    end
end

function AntiCheat:LogTransaction(player, action, itemId, quantity)
    local userId = tostring(player.UserId)
    self._playerLogs[userId] = self._playerLogs[userId] or { changes = {}, startTime = tick() }
    table.insert(self._playerLogs[userId].changes, {
        action = action,
        item = itemId,
        quantity = quantity,
        time = tick()
    })
end

function AntiCheat:FlagPlayer(player, reason, details)
    local uid = tostring(player.UserId)
    self._suspiciousPlayers[uid] = { reason = reason, details = details, time = tick() }
    self._eventBus:Fire("SuspiciousActivity", player, reason, details)
    warn(string.format("AntiCheat: Flagged %s (%s) for %s: %s", player.Name, uid, tostring(reason), tostring(details)))
end

function AntiCheat:IsPlayerFlagged(player)
    return self._suspiciousPlayers[tostring(player.UserId)] ~= nil
end

-- ============================================
-- Bootstrap & initialization
-- ============================================
local function InitializeSystems()
    local eventBus = EventBus.new()
    local dataManager = PlayerDataManager.new("PlayerData_v2", eventBus, { cacheSize = 200, autoSaveInterval = 120, dataVersion = 2 })
    local inventorySystem = InventorySystem.new(dataManager, eventBus)
    local leaderboardManager = LeaderboardManager.new(dataManager)
    local antiCheat = AntiCheat.new(eventBus)

    -- create leaderboards under a safe parent (Players service -> shared folder)
    local leaderstatsFolder = Instance.new("Folder")
    leaderstatsFolder.Name = "Leaderboards"
    leaderstatsFolder.Parent = workspace -- change as needed
    leaderboardManager:CreateLeaderboard("Coins", "coins", leaderstatsFolder)
    leaderboardManager:CreateLeaderboard("Level", "level", leaderstatsFolder)

    local players = PlayersService

    players.PlayerAdded:Connect(function(player)
        task.spawn(function()
            local data = dataManager:LoadPlayerData(player)
            if data then
                leaderboardManager:UpdatePlayerStat(player, "Coins", data.coins)
                leaderboardManager:UpdatePlayerStat(player, "Level", data.level)
            end
        end)
    end)

    players.PlayerRemoving:Connect(function(player)
        dataManager:RemovePlayer(player)
    end)

    game:BindToClose(function()
        dataManager:Shutdown()
    end)

    eventBus:Connect("SuspiciousActivity", function(player, reason, details)
        -- hook to external logging / moderation systems here
    end)

    print("Advanced Modular Systems Initialized Successfully")
    print(string.format("EventBus Listeners: %d", eventBus:GetListenerCount()))

    return {
        EventBus = eventBus,
        DataManager = dataManager,
        Inventory = inventorySystem,
        Leaderboard = leaderboardManager,
        AntiCheat = antiCheat
    }
end

local Systems = InitializeSystems()
_G.PlayerSystems = Systems

return Systems
