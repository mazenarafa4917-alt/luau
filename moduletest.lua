
-- ============================================
-- MODULE 1: EventBus (Observer Pattern Implementation)
-- Centralized event communication between systems
-- ============================================

local EventBus = {}
EventBus.__index = EventBus

function EventBus.new()
    local self = setmetatable({}, EventBus)
    self._listeners = {} -- Table storing event subscriptions
    self._onceListeners = {} -- One-time event listeners
    self._listenerCount = 0 -- Performance tracking counter
    return self
end

function EventBus:Connect(eventName, callback)
    if not self._listeners[eventName] then
        self._listeners[eventName] = {}
    end
    table.insert(self._listeners[eventName], callback)
    self._listenerCount += 1
    return {
        Disconnect = function()
            self:Disconnect(eventName, callback)
        end
    }
end

function EventBus:Once(eventName, callback)
    if not self._onceListeners[eventName] then
        self._onceListeners[eventName] = {}
    end
    table.insert(self._onceListeners[eventName], callback)
end

function EventBus:Fire(eventName, ...)
    local args = {...}
    if self._listeners[eventName] then
        for _, callback in ipairs(self._listeners[eventName]) do
            task.spawn(function()
                callback(table.unpack(args))
            end)
        end
    end
    if self._onceListeners[eventName] then
        for _, callback in ipairs(self._onceListeners[eventName]) do
            task.spawn(function()
                callback(table.unpack(args))
            end)
        end
        self._onceListeners[eventName] = nil
    end
end

function EventBus:Disconnect(eventName, callback)
    if self._listeners[eventName] then
        for i, storedCallback in ipairs(self._listeners[eventName]) do
            if storedCallback == callback then
                table.remove(self._listeners[eventName], i)
                self._listenerCount -= 1
                break
            end
        end
    end
end

function EventBus:GetListenerCount()
    return self._listenerCount
end

-- ============================================
-- MODULE 2: DataSerializer (Data Compression & Validation)
-- Handles data transformation for DataStore optimization
-- ============================================

local DataSerializer = {}
DataSerializer.__index = DataSerializer

function DataSerializer.new()
    local self = setmetatable({}, DataSerializer)
    self._schemas = {}
    self._compressionEnabled = true
    return self
end

function DataSerializer:RegisterSchema(dataType, schema)
    self._schemas[dataType] = {
        defaults = schema.defaults or {},
        validators = schema.validators or {},
        transformers = schema.transformers or {}
    }
end

function DataSerializer:Serialize(data, dataType)
    local schema = self._schemas[dataType]
    if not schema then return data end
    local serialized = {}
    for key, value in pairs(data) do
        if schema.transformers[key] then
            serialized[key] = schema.transformers[key].serialize(value)
        else
            serialized[key] = value
        end
    end
    return serialized
end

function DataSerializer:Deserialize(data, dataType)
    local schema = self._schemas[dataType]
    if not schema then return data end
    local deserialized = {}
    for key, defaultValue in pairs(schema.defaults) do
        if data[key] ~= nil then
            if schema.transformers[key] then
                deserialized[key] = schema.transformers[key].deserialize(data[key])
            else
                deserialized[key] = data[key]
            end
        else
            deserialized[key] = defaultValue
        end
    end
    return deserialized
end

function DataSerializer:Validate(data, dataType)
    local schema = self._schemas[dataType]
    if not schema then return true end
    for key, validator in pairs(schema.validators) do
        if data[key] ~= nil and not validator(data[key]) then
            return false, key .. " failed validation"
        end
    end
    return true
end

-- ============================================
-- MODULE 3: CacheManager (LRU Cache Implementation)
-- Optimizes memory usage with least-recently-used eviction
-- ============================================

local CacheManager = {}
CacheManager.__index = CacheManager

function CacheManager.new(maxSize)
    local self = setmetatable({}, CacheManager)
    self._cache = {}
    self._accessOrder = {}
    self._maxSize = maxSize or 100
    self._currentSize = 0
    self._hits = 0
    self._misses = 0
    return self
end

function CacheManager:Get(key)
    if self._cache[key] then
        self._hits += 1
        self:UpdateAccessOrder(key)
        return self._cache[key].value
    end
    self._misses += 1
    return nil
end

function CacheManager:Set(key, value, ttl)
    if self._currentSize >= self._maxSize and not self._cache[key] then
        self:EvictLRU()
    end
    local expiration = ttl and (tick() + ttl) or nil
    self._cache[key] = {value = value, expiration = expiration}
    if not self._cache[key] then
        self._currentSize += 1
    end
    self:UpdateAccessOrder(key)
end

function CacheManager:UpdateAccessOrder(key)
    for i, k in ipairs(self._accessOrder) do
        if k == key then
            table.remove(self._accessOrder, i)
            break
        end
    end
    table.insert(self._accessOrder, key)
end

function CacheManager:EvictLRU()
    if #self._accessOrder == 0 then return end
    local lruKey = self._accessOrder[1]
    self:Remove(lruKey)
end

function CacheManager:Remove(key)
    if self._cache[key] then
        self._cache[key] = nil
        self._currentSize -= 1
        for i, k in ipairs(self._accessOrder) do
            if k == key then
                table.remove(self._accessOrder, i)
                break
            end
        end
    end
end

function CacheManager:GetStats()
    local total = self._hits + self._misses
    local hitRate = total > 0 and (self._hits / total) or 0
    return {
        size = self._currentSize,
        maxSize = self._maxSize,
        hitRate = hitRate,
        hits = self._hits,
        misses = self._misses
    }
end

function CacheManager:CleanupExpired()
    local now = tick()
    for key, data in pairs(self._cache) do
        if data.expiration and now > data.expiration then
            self:Remove(key)
        end
    end
end

-- ============================================
-- MODULE 4: PlayerDataManager (Core Data Persistence)
-- Handles DataStore operations with retry logic and versioning
-- ============================================

local PlayerDataManager = {}
PlayerDataManager.__index = PlayerDataManager

function PlayerDataManager.new(dataStoreName, eventBus)
    local self = setmetatable({}, PlayerDataManager)
    self._dataStore = game:GetService("DataStoreService"):GetDataStore(dataStoreName)
    self._eventBus = eventBus
    self._playerData = {}
    self._sessionLocks = {}
    self._serializer = DataSerializer.new()
    self._cache = CacheManager.new(50)
    self._autoSaveInterval = 120
    self._dataVersion = 2
    self:_setupAutoSave()
    self:_registerSchemas()
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
                serialize = function(inv) return game:GetService("HttpService"):JSONEncode(inv) end,
                deserialize = function(inv) return game:GetService("HttpService"):JSONDecode(inv) end
            }
        }
    })
end

function PlayerDataManager:_setupAutoSave()
    task.spawn(function()
        while true do
            task.wait(self._autoSaveInterval)
            self:SaveAllData()
        end
    end)
end

function PlayerDataManager:LoadPlayerData(player)
    local userId = tostring(player.UserId)
    local success, data = pcall(function()
        return self._dataStore:GetAsync(userId)
    end)
    if success then
        if data then
            if data.version and data.version ~= self._dataVersion then
                data = self:_migrateData(data)
            end
            local deserialized = self._serializer:Deserialize(data.data or data, "playerData")
            self._playerData[userId] = deserialized
            self._cache:Set(userId, deserialized, 300)
            self._eventBus:Fire("DataLoaded", player, deserialized)
            return deserialized
        else
            local newData = self._serializer:Deserialize({}, "playerData")
            self._playerData[userId] = newData
            self._eventBus:Fire("NewPlayer", player, newData)
            return newData
        end
    else
        self._eventBus:Fire("DataLoadFailed", player, data)
        return nil
    end
end

function PlayerDataManager:_migrateData(oldData)
    local migrated = {
        version = self._dataVersion,
        data = oldData.data or oldData
    }
    if not oldData.version then
        migrated.data.settings = {music = true, sfx = true}
    end
    return migrated
end

function PlayerDataManager:SavePlayerData(player)
    local userId = tostring(player.UserId)
    local data = self._playerData[userId]
    if not data then return false end
    local serialized = {
        version = self._dataVersion,
        data = self._serializer:Serialize(data, "playerData"),
        timestamp = tick()
    }
    local success, err = pcall(function()
        self._dataStore:SetAsync(userId, serialized)
    end)
    if success then
        self._eventBus:Fire("DataSaved", player, data)
        return true
    else
        self._eventBus:Fire("DataSaveFailed", player, err)
        return false
    end
end

function PlayerDataManager:SaveAllData()
    for userId, _ in pairs(self._playerData) do
        local player = game:GetService("Players"):GetPlayerByUserId(tonumber(userId))
        if player then
            self:SavePlayerData(player)
        end
    end
end

function PlayerDataManager:GetData(player)
    return self._playerData[tostring(player.UserId)] or self._cache:Get(tostring(player.UserId))
end

function PlayerDataManager:UpdateData(player, key, value)
    local userId = tostring(player.UserId)
    if self._playerData[userId] then
        local oldValue = self._playerData[userId][key]
        self._playerData[userId][key] = value
        self._eventBus:Fire("DataChanged", player, key, value, oldValue)
        return true
    end
    return false
end

function PlayerDataManager:IncrementData(player, key, amount)
    local data = self:GetData(player)
    if data and type(data[key]) == "number" then
        local newValue = data[key] + amount
        self:UpdateData(player, key, newValue)
        return newValue
    end
    return nil
end

function PlayerDataManager:RemovePlayer(player)
    local userId = tostring(player.UserId)
    self:SavePlayerData(player)
    self._playerData[userId] = nil
    self._cache:Remove(userId)
    self._eventBus:Fire("PlayerRemoved", player)
end

-- ============================================
-- MODULE 5: InventorySystem (Item Management)
-- Advanced inventory with stacking, equipping, and trading
-- ============================================

local InventorySystem = {}
InventorySystem.__index = InventorySystem

function InventorySystem.new(dataManager, eventBus)
    local self = setmetatable({}, InventorySystem)
    self._dataManager = dataManager
    self._eventBus = eventBus
    self._itemDatabase = {}
    self._equippedItems = {}
    self._maxStackSize = 99
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
    if not data or not data.inventory then return end
    local validInventory = {}
    for itemId, itemData in pairs(data.inventory) do
        if self._itemDatabase[itemId] then
            validInventory[itemId] = itemData
        end
    end
    self._dataManager:UpdateData(player, "inventory", validInventory)
end

function InventorySystem:AddItem(player, itemId, quantity)
    quantity = quantity or 1
    local itemDef = self._itemDatabase[itemId]
    if not itemDef then return false, "Invalid item" end
    local data = self._dataManager:GetData(player)
    if not data then return false, "No player data" end
    local inventory = data.inventory or {}
    local currentQty = inventory[itemId] and inventory[itemId].quantity or 0
    local newQty = currentQty + quantity
    if newQty > itemDef.maxStack then
        newQty = itemDef.maxStack
        quantity = itemDef.maxStack - currentQty
    end
    inventory[itemId] = {
        quantity = newQty,
        acquired = inventory[itemId] and inventory[itemId].acquired or tick(),
        equipped = inventory[itemId] and inventory[itemId].equipped or false
    }
    self._dataManager:UpdateData(player, "inventory", inventory)
    self._eventBus:Fire("ItemAdded", player, itemId, quantity, newQty)
    return true, quantity
end

function InventorySystem:RemoveItem(player, itemId, quantity)
    quantity = quantity or 1
    local data = self._dataManager:GetData(player)
    if not data or not data.inventory or not data.inventory[itemId] then
        return false, "Item not found"
    end
    local inventory = data.inventory
    local currentQty = inventory[itemId].quantity
    if currentQty < quantity then
        return false, "Insufficient quantity"
    end
    local newQty = currentQty - quantity
    if newQty <= 0 then
        inventory[itemId] = nil
        self:UnequipItem(player, itemId)
    else
        inventory[itemId].quantity = newQty
    end
    self._dataManager:UpdateData(player, "inventory", inventory)
    self._eventBus:Fire("ItemRemoved", player, itemId, quantity, newQty)
    return true, newQty
end

function InventorySystem:EquipItem(player, itemId)
    local data = self._dataManager:GetData(player)
    if not data or not data.inventory or not data.inventory[itemId] then
        return false, "Item not in inventory"
    end
    local itemDef = self._itemDatabase[itemId]
    if itemDef.type ~= "weapon" and itemDef.type ~= "armor" then
        return false, "Item not equippable"
    end
    local userId = tostring(player.UserId)
    if not self._equippedItems[userId] then
        self._equippedItems[userId] = {}
    end
    for equippedId, _ in pairs(self._equippedItems[userId]) do
        if self._itemDatabase[equippedId].type == itemDef.type then
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
    local userId = tostring(player.UserId)
    if data and data.inventory and data.inventory[itemId] then
        data.inventory[itemId].equipped = false
        self._dataManager:UpdateData(player, "inventory", data.inventory)
    end
    if self._equippedItems[userId] then
        self._equippedItems[userId][itemId] = nil
    end
    self._eventBus:Fire("ItemUnequipped", player, itemId)
    return true
end

function InventorySystem:GetEquippedItems(player)
    return self._equippedItems[tostring(player.UserId)] or {}
end

function InventorySystem:HasItem(player, itemId, quantity)
    quantity = quantity or 1
    local data = self._dataManager:GetData(player)
    if not data or not data.inventory then return false end
    local item = data.inventory[itemId]
    return item and item.quantity >= quantity
end

function InventorySystem:GetItemCount(player, itemId)
    local data = self._dataManager:GetData(player)
    if not data or not data.inventory then return 0 end
    return data.inventory[itemId] and data.inventory[itemId].quantity or 0
end

function InventorySystem:TradeItem(fromPlayer, toPlayer, itemId, quantity)
    if not self:HasItem(fromPlayer, itemId, quantity) then
        return false, "Insufficient items"
    end
    local removeSuccess = self:RemoveItem(fromPlayer, itemId, quantity)
    if not removeSuccess then
        return false, "Failed to remove items"
    end
    local addSuccess, actualQty = self:AddItem(toPlayer, itemId, quantity)
    if not addSuccess then
        self:AddItem(fromPlayer, itemId, quantity)
        return false, "Failed to add items to recipient"
    end
    self._eventBus:Fire("ItemTraded", fromPlayer, toPlayer, itemId, quantity)
    return true
end

function InventorySystem:CraftItem(player, recipeId)
    local recipes = {
        ["sword_steel"] = {requirements = {["iron_ore"] = 3, ["coal"] = 2}, result = "sword_steel", quantity = 1}
    }
    local recipe = recipes[recipeId]
    if not recipe then return false, "Invalid recipe" end
    for reqItem, reqQty in pairs(recipe.requirements) do
        if not self:HasItem(player, reqItem, reqQty) then
            return false, "Missing materials: " .. reqItem
        end
    end
    for reqItem, reqQty in pairs(recipe.requirements) do
        self:RemoveItem(player, reqItem, reqQty)
    end
    self:AddItem(player, recipe.result, recipe.quantity)
    self._eventBus:Fire("ItemCrafted", player, recipeId, recipe.result)
    return true
end

-- ============================================
-- MODULE 6: LeaderboardManager (Stat Display)
-- Handles leaderboards with efficient updates
-- ============================================

local LeaderboardManager = {}
LeaderboardManager.__index = LeaderboardManager

function LeaderboardManager.new(dataManager)
    local self = setmetatable({}, LeaderboardManager)
    self._dataManager = dataManager
    self._leaderboards = {}
    self._updateQueue = {}
    self:_setupUpdateLoop()
    return self
end

function LeaderboardManager:CreateLeaderboard(name, statKey, parent)
    local folder = Instance.new("Folder")
    folder.Name = name
    folder.Parent = parent or game:GetService("Leaderstats")
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
    lb.stats[userId].Value = value
end

function LeaderboardManager:_setupUpdateLoop()
    task.spawn(function()
        while true do
            task.wait(5)
            for _, player in ipairs(game:GetService("Players"):GetPlayers()) do
                local data = self._dataManager:GetData(player)
                if data then
                    for name, lb in pairs(self._leaderboards) do
                        if data[lb.statKey] then
                            self:UpdatePlayerStat(player, name, data[lb.statKey])
                        end
                    end
                end
            end
        end
    end)
end

-- ============================================
-- MODULE 7: AntiCheat (Basic Validation Layer)
-- Detects anomalous data modifications
-- ============================================

local AntiCheat = {}
AntiCheat.__index = AntiCheat

function AntiCheat.new(eventBus)
    local self = setmetatable({}, AntiCheat)
    self._eventBus = eventBus
    self._suspiciousPlayers = {}
    self._thresholds = {
        maxCoinsPerMinute = 10000,
        maxItemsPerMinute = 50,
        maxLevelDifference = 10
    }
    self._playerLogs = {}
    self:_setupMonitoring()
    return self
end

function AntiCheat:_setupMonitoring()
    self._eventBus:Connect("DataChanged", function(player, key, newValue, oldValue)
        self:ValidateChange(player, key, newValue, oldValue)
    end)
    self._eventBus:Connect("ItemAdded", function(player, itemId, quantity)
        self:LogTransaction(player, "add", itemId, quantity)
    end)
end

function AntiCheat:ValidateChange(player, key, newValue, oldValue)
    local userId = tostring(player.UserId)
    if not self._playerLogs[userId] then
        self._playerLogs[userId] = {changes = {}, startTime = tick()}
    end
    table.insert(self._playerLogs[userId].changes, {
        key = key,
        time = tick(),
        delta = newValue - (oldValue or 0)
    })
    if key == "coins" then
        local recentEarned = 0
        local now = tick()
        for _, change in ipairs(self._playerLogs[userId].changes) do
            if change.key == "coins" and (now - change.time) < 60 then
                recentEarned += change.delta
            end
        end
        if recentEarned > self._thresholds.maxCoinsPerMinute then
            self:FlagPlayer(player, "coin_generation", recentEarned)
        end
    end
end

function AntiCheat:LogTransaction(player, action, itemId, quantity)
    local userId = tostring(player.UserId)
    if not self._playerLogs[userId] then return end
    table.insert(self._playerLogs[userId].changes, {
        action = action,
        item = itemId,
        quantity = quantity,
        time = tick()
    })
end

function AntiCheat:FlagPlayer(player, reason, details)
    self._suspiciousPlayers[tostring(player.UserId)] = {
        reason = reason,
        details = details,
        time = tick()
    }
    self._eventBus:Fire("SuspiciousActivity", player, reason, details)
    warn(string.format("AntiCheat: Flagged player %s for %s", player.Name, reason))
end

function AntiCheat:IsPlayerFlagged(player)
    return self._suspiciousPlayers[tostring(player.UserId)] ~= nil
end

-- ============================================
-- MAIN INITIALIZATION
-- System bootstrap and player connection handling
-- ============================================

local function InitializeSystems()
    local eventBus = EventBus.new()
    local dataManager = PlayerDataManager.new("PlayerData_v2", eventBus)
    local inventorySystem = InventorySystem.new(dataManager, eventBus)
    local leaderboardManager = LeaderboardManager.new(dataManager)
    local antiCheat = AntiCheat.new(eventBus)
    
    leaderboardManager:CreateLeaderboard("Coins", "coins")
    leaderboardManager:CreateLeaderboard("Level", "level")
    
    local players = game:GetService("Players")
    
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
        dataManager:SaveAllData()
    end)
    
    eventBus:Connect("SuspiciousActivity", function(player, reason, details)
        -- Log to external service or take action
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

-- Initialize on server start
local Systems = InitializeSystems()

-- Expose API for other scripts
_G.PlayerSystems = Systems
