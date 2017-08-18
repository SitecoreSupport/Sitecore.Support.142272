namespace Sitecore.Support.Analytics.Data.DataAccess.MongoDb
{
    using Sitecore.Analytics.Data;
    using Sitecore.Analytics.Data.DataAccess.MongoDb;
    using Sitecore.Analytics.DataAccess;
    using Sitecore.Analytics.Model;
    using Sitecore.Configuration;
    using Sitecore.Data;
    using Sitecore.Diagnostics;
    using System;
    using System.Collections.Generic;
    using System.Collections.Specialized;
    using System.Configuration;

    public class MongoDbDataAdapterProvider : Sitecore.Analytics.Data.DataAccess.MongoDb.MongoDbDataAdapterProvider
    {
        private MongoDbDriver driver;

        private MongoDbInteractionStorage customInteractionStorage;

        private readonly Func<string, MongoDbDriver> driverFactory;

        private MongoDBClassifications classifications;

        internal static readonly TimeSpan IdentifierLockTimeout;

        public override TrafficTypeBase TrafficType
        {
            get
            {
                return new MongoTrafficType(this.driver);
            }
        }

        public override VisitorClassificationBase VisitorClassification
        {
            get
            {
                if (this.classifications == null)
                {
                    this.classifications = new MongoDBClassifications(this.driver);
                }
                return this.classifications;
            }
        }

        static MongoDbDataAdapterProvider()
        {
            MongoDbDataAdapterProvider.IdentifierLockTimeout = TimeSpan.FromSeconds(1.0);
            MongoDbDataAdapterProvider.IdentifierLockTimeout = TimeSpan.FromSeconds(1.0);
        }

        public MongoDbDataAdapterProvider() : this(new Func<string, MongoDbDriver>(MongoDbDataAdapterProvider.DefaultDriverFactory))
        {
        }

        public MongoDbDataAdapterProvider(Func<string, MongoDbDriver> driverFactory)
        {
            Assert.ArgumentNotNull(driverFactory, "driverFactory");
            this.driverFactory = driverFactory;
        }

        private static MongoDbDriver DefaultDriverFactory(string connectionString)
        {
            MongoDbDriver mongoDbDriver = Factory.CreateObject("mongo/driver", new string[]
            {
                connectionString
            }, true) as MongoDbDriver;
            Assert.IsNotNull(mongoDbDriver, "driver should not be null");
            return mongoDbDriver;
        }

        public override DataStorage<TEntity, TKey> GetDataStorage<TEntity, TKey>(string collectionName)
        {
            Assert.ArgumentNotNull(collectionName, "collectionName");
            MongoDbObjectMapper.Instance.AssertTypeIsRegistered<TEntity>();
            MongoDbObjectMapper.Instance.AssertCanUseCollectionWith<TEntity>(collectionName);
            return new MongoDbStorage<TEntity, TKey>(this.driver[collectionName]);
        }

        public override void Initialize(string name, NameValueCollection config)
        {
            Assert.ArgumentNotNull(name, "name");
            Assert.ArgumentNotNull(config, "config");
            base.Initialize(name, config);
            string text = config.Get("connectionStringName");
            bool flag = text != null;
            string arg;
            if (flag)
            {
                ConnectionStringSettings connectionStringSettings = ConfigurationManager.ConnectionStrings[text];
                bool flag2 = connectionStringSettings == null;
                if (flag2)
                {
                    throw new Exception("Connection string '" + text + "' could not be found.");
                }
                arg = connectionStringSettings.ConnectionString;
            }
            else
            {
                arg = config.Get("connectionString");
            }
            this.driver = this.driverFactory(arg);
            this.customInteractionStorage = new MongoDbInteractionStorage(this.driver.Interactions);
        }

        public override void Remove(IUpdatableObject container)
        {
            Assert.ArgumentNotNull(container, "container");
            string instanceName = Settings.InstanceName;
            try
            {
                foreach (object current in container.GetRemovableParts())
                {
                    this.driver.Remove(current);
                }
            }
            catch (Exception)
            {
                this.SystemHealth.SetLastFailureDate(instanceName, "Database remove", DateTime.UtcNow);
                throw;
            }
            this.SystemHealth.SetLastSuccessDate(instanceName, "Database remove", DateTime.UtcNow);
        }

        public override void Update(IUpdatableObject container)
        {
            Assert.ArgumentNotNull(container, "container");
            string instanceName = Settings.InstanceName;
            try
            {
                foreach (object current in container.GetParts())
                {
                    this.driver.Save(current);
                }
            }
            catch (Exception)
            {
                this.SystemHealth.SetLastFailureDate(instanceName, "Database submit", DateTime.UtcNow);
                throw;
            }
            this.SystemHealth.SetLastSuccessDate(instanceName, "Database submit", DateTime.UtcNow);
        }

        public override IList<TInteraction> LoadInteractions<TInteraction>(InteractionLoadOptions options)
        {
            Assert.ArgumentNotNull(options, "options");
            IList<TInteraction> list = this.customInteractionStorage.LoadInteractions<TInteraction>(options);
            foreach (TInteraction current in list)
            {
                VisitData visitData = current as VisitData;
                bool flag = visitData != null && current.ChannelId == Guid.Empty;
                if (flag)
                {
                    Guid? guid = TrafficTypeConverter.ConvertToChannelId(visitData.TrafficType);
                    bool hasValue = guid.HasValue;
                    if (hasValue)
                    {
                        current.ChannelId = guid.Value;
                    }
                }
            }
            return list;
        }

        public override IList<VisitData> LoadVisits(InteractionLoadOptions options)
        {
            Assert.ArgumentNotNull(options, "options");
            IList<VisitData> list = this.customInteractionStorage.LoadVisits(options);
            foreach (VisitData current in list)
            {
                bool flag = current.ChannelId == Guid.Empty;
                if (flag)
                {
                    Guid? guid = TrafficTypeConverter.ConvertToChannelId(current.TrafficType);
                    bool hasValue = guid.HasValue;
                    if (hasValue)
                    {
                        current.ChannelId = guid.Value;
                    }
                }
            }
            return list;
        }

        public override void MergeVisits(Guid dyingContactId, Guid survivingContactId)
        {
            this.customInteractionStorage.MergeVisits(dyingContactId, survivingContactId);
        }

        public override void RenumberInteractions(ID contactId)
        {
            Assert.ArgumentNotNull(contactId, "contactId");
            Guid contactId2 = contactId.ToGuid();
            this.customInteractionStorage.RenumberInteractions(contactId2);
        }
    }
}
