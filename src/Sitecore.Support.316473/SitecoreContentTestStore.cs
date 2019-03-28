using System.Collections.Concurrent;
using System.Collections.Generic;
using Sitecore.Caching.Generics;
using Sitecore.Collections;
using Sitecore.Configuration;
using Sitecore.ContentTesting;
using Sitecore.Data;
using Sitecore.Data.Items;
using Sitecore.Data.Engines.DataCommands;
using Sitecore.Data.Events;
using Sitecore.Eventing;
using Sitecore.Eventing.Remote;


namespace Sitecore.Support.ContentTesting.Data
{
  public class SitecoreContentTestStore : Sitecore.ContentTesting.Data.SitecoreContentTestStore
  {
    public class IsTestRunningCache : CustomCache<ID>
    {
      protected internal class IndexedCacheKeyContainer
      {
        private readonly ConcurrentDictionary<ID, ID> itemIdIndex;
        private readonly ConcurrentDictionary<ID, ConcurrentSet<ID>> testIdIndex;

        public IndexedCacheKeyContainer()
        {
          itemIdIndex = new ConcurrentDictionary<ID, ID>();
          testIdIndex = new ConcurrentDictionary<ID, ConcurrentSet<ID>>();
        }

        public void UpdateIndexes(ID itemId, ID testId)
        {
          if (!ID.IsNullOrEmpty(itemId))
          {
            ID orAdd = itemIdIndex.GetOrAdd(itemId,
              (ID objKey) => testId);
            orAdd = testId;
          }

          if (!ID.IsNullOrEmpty(testId))
          {
            ConcurrentSet<ID> orAdd = testIdIndex.GetOrAdd(testId,
              (ID objKey) => new ConcurrentSet<ID>());
            orAdd.Add(itemId);
          }
        }

        public void RemoveKeysByItemId(ID itemId)
        {
          if (!ID.IsNullOrEmpty(itemId))
          {
            ID testId;
            itemIdIndex.TryRemove(itemId, out testId);
            if (!ID.IsNullOrEmpty(testId))
            {
              var keys = new ConcurrentSet<ID>();
              if (testIdIndex.TryGetValue(testId, out keys))
              {
                keys.Remove(itemId);
              }
            }
          }
        }

        public IEnumerable<ID> RemoveKeysByTestId(ID testId)
        {
          List<ID> removedKeys = new List<ID>();
          if (!ID.IsNullOrEmpty(testId))
          {
            ConcurrentSet<ID> keys;
            testIdIndex.TryRemove(testId, out keys);
            if (keys != null)
            {
              removedKeys.AddRange(keys);
              foreach (ID key in keys)
              {
                ID removeCandidateId;
                if (itemIdIndex.TryGetValue(key, out removeCandidateId) && testId == removeCandidateId)
                {
                  itemIdIndex.TryRemove(key, out removeCandidateId);
                }
              }
            }
          }

          return removedKeys;
        }
      }

      private IndexedCacheKeyContainer indexContainer;

      public IsTestRunningCache(string name, long maxSize, Database database) : base(database.Name+name, maxSize)
      {
        indexContainer = new IndexedCacheKeyContainer();
        var dataEngine = database.Engines.DataEngine;
        dataEngine.SavedItem += DataEngine_SavedItem;
        dataEngine.DeletedItem += DataEngine_DeletedItem;
        EventManager.Subscribe<PublishEndRemoteEvent>(OnPublishEndRemoteEvent);
      }

      private void OnPublishEndRemoteEvent(PublishEndRemoteEvent obj)
      {
        this.Clear();
      }

      private void DataEngine_DeletedItem(object sender, ExecutedEventArgs<DeleteItemCommand> e)
      {
        var item = e.Command.Item;
        if (item.Paths.FullPath.StartsWith("/sitecore/system/Marketing Control Panel/Test Lab"))
        {
          HandleTest(item);
        }
        else
        {
          RemoveByItemdId(item.ID);
        }
      }

      private void DataEngine_SavedItem(object sender, ExecutedEventArgs<SaveItemCommand> e)
      {
        var item = e.Command.Item;
        if (item.Paths.FullPath.StartsWith("/sitecore/system/Marketing Control Panel/Test Lab"))
        {
          HandleTest(item);
        }
        else
        {
          RemoveByItemdId(item.ID);
        }
      }

      private void HandleTest(Item item)
      {
        if (item.TemplateID == ID.Parse("{45FB02E9-70B3-4CFE-8050-06EAD4B5DB3E}"))
        {
          RemoveByTestId(item.ID);
          return;
        }

        var parent = item.Axes.SelectSingleItem("ancestor::*[@@templateid='{45FB02E9-70B3-4CFE-8050-06EAD4B5DB3E}']");
        if (parent != null)
        {
          RemoveByTestId(parent.ID);
        }
      }

      public void Add(ID itemId, ID testId, bool? isRunning)
      {
        this.SetObject(itemId, isRunning);
        indexContainer.UpdateIndexes(itemId,testId);
      }

      public bool? Get(ID itemId)
      {
        return this.GetObject(itemId) as bool?;
        ;
      }

      public void RemoveByItemdId(ID itemId)
      {
        this.Remove(itemId);
      }

      public void RemoveByTestId(ID testId)
      {
        var keys = indexContainer.RemoveKeysByTestId(testId);
        foreach (ID key in keys)
        {
          this.Remove(key);
        }
      }
    }

    public SitecoreContentTestStore() : base()
    {
      var databases = Factory.GetDatabases();
      lock (cachesSyncRoot)
      {
        foreach (var database in databases)
        {
          if (database.Name != "core" && database.Name != "filesystem" && !caches.ContainsKey(database.Name))
          {
            IsTestRunningCache cache = new IsTestRunningCache("[IsTestRunningCache]",
              StringUtil.ParseSizeString(Settings.GetSetting("ContentTesting.TestConfigurationCacheSize", "50MB")),
              database);
            caches.Add(database.Name, cache);
          }
        }
      }
    }

    private static Dictionary<string, IsTestRunningCache> caches = new Dictionary<string, IsTestRunningCache>();
    private static object cachesSyncRoot = new object();
    
    public override bool IsTestRunning(Item contentItem)
    {
      IsTestRunningCache cache;
      if (caches.TryGetValue(contentItem.Database.Name, out cache))
      {
        bool? isRunning;
        bool result = false;
        isRunning = cache.Get(contentItem.ID);
        if (isRunning != null)
        {
          result = (bool) isRunning;
        }
        else
        {
          ITestConfiguration testConfiguration = LoadTestForItem(contentItem, true);
          if (testConfiguration != null && testConfiguration.TestDefinitionItem != null)
          {
            result = testConfiguration.TestDefinitionItem.IsRunning;
          }

          ID testId = testConfiguration?.TestDefinitionItem?.ID ?? ID.Null;
          isRunning = result;
          cache.Add(contentItem.ID, testId, isRunning);
        }

        return result;
      }

      return base.IsTestRunning(contentItem);
    }
  }
}