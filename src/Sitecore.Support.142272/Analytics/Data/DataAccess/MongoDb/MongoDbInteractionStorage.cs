namespace Sitecore.Support.Analytics.Data.DataAccess.MongoDb
{
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization;
    using MongoDB.Bson.Serialization.Conventions;
    using MongoDB.Driver;
    using MongoDB.Driver.Builders;
    using Sitecore.Analytics.Data.DataAccess.MongoDb;
    using Sitecore.Analytics.DataAccess;
    using Sitecore.Analytics.Model;
    using Sitecore.Diagnostics;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;

    internal class MongoDbInteractionStorage
    {
        private readonly MongoDbCollection interactions;

        public MongoDbInteractionStorage(MongoDbCollection collection)
        {
            Assert.ArgumentNotNull(collection, "collection");
            this.interactions = collection;
        }

        private bool GetIsRunningOnSingleServer()
        {
            bool flag = false;
            try
            {
                flag = this.interactions.ShardingInformation.GetNumberOfChunks() == 0;
            }
            catch (Exception exception)
            {
                Log.Warn("Cannot obtain number of chunks from config database", exception, this);
            }
            return flag;
        }

        public IList<TInteraction> LoadInteractions<TInteraction>(InteractionLoadOptions options) where TInteraction : InteractionData
        {
            IMongoQuery query;
            Assert.ArgumentNotNull(options, "options");
            if (options.InteractionId != Guid.Empty)
            {
                query = Query.EQ("_id", BsonValue.Create(options.InteractionId));
                if (options.ContactId != Guid.Empty)
                {
                    IMongoQuery[] queries = new IMongoQuery[] { query, Query.EQ("ContactId", options.ContactId) };
                    query = Query.And(queries);
                }
                MongoCursor<TInteraction> source = this.interactions.FindAs<TInteraction>(query);
                if (source != null)
                {
                    return source.ToList<TInteraction>();
                }
                return new List<TInteraction>();
            }
            if (options.VisitsToLoad == 0)
            {
                return new TInteraction[0];
            }
            query = Query.EQ("ContactId", BsonValue.Create(options.ContactId));
            if (options.MinimumVisitDate.HasValue)
            {
                IMongoQuery[] queryArray2 = new IMongoQuery[] { query, Query.GTE("StartDateTime", options.MinimumVisitDate) };
                query = Query.And(queryArray2);
            }
            if (options.MaximumVisitDate.HasValue)
            {
                IMongoQuery[] queryArray3 = new IMongoQuery[] { query, Query.LT("StartDateTime", options.MaximumVisitDate) };
                query = Query.And(queryArray3);
            }
            if (typeof(TInteraction) != typeof(InteractionData))
            {
                IDiscriminatorConvention discriminatorConvention = BsonSerializer.LookupDiscriminatorConvention(typeof(TInteraction));
                object[] args = new object[] { typeof(TInteraction) };
                Assert.IsNotNull(discriminatorConvention, "Failed to detect discriminator for type {0}", args);
                BsonValue discriminator = discriminatorConvention.GetDiscriminator(typeof(InteractionData), typeof(TInteraction));
                IMongoQuery[] queryArray4 = new IMongoQuery[] { Query.EQ("_t", BsonValue.Create(discriminator)), query };
                query = Query.And(queryArray4);
            }
            MongoCursor<TInteraction> cursor = this.interactions.FindAs<TInteraction>(query);
            if (cursor == null)
            {
                return new List<TInteraction>();
            }
            string[] keys = new string[] { "StartDateTime" };
            return cursor.SetSortOrder(new SortByBuilder().Descending(keys)).Skip<TInteraction>(options.VisitsToSkip).Take<TInteraction>(options.VisitsToLoad).ToList<TInteraction>();
        }

        public IList<VisitData> LoadVisits(InteractionLoadOptions options)
        {
            Assert.ArgumentNotNull(options, "options");
            return this.LoadInteractions<VisitData>(options);
        }

        public void MergeVisits(Guid dyingContactId, Guid survivingContactId)
        {
            MongoCursor<VisitData> mongoCursor = this.interactions.FindAs<VisitData>(Query.And(Query.Or(Query.EQ("ContactId", BsonValue.Create((object)dyingContactId)), Query.EQ("ContactId", BsonValue.Create((object)survivingContactId))), Query.EQ("_t", BsonValue.Create((object)typeof(VisitData).Name))));
            if (mongoCursor == null)
            {
                throw new DatabaseNotAvailableException();
            }
            VisitData[] array = mongoCursor.SetSortOrder("StartDateTime", "_id").SetFields((IMongoFields)Fields.Include("ContactId", "ContactVisitIndex")).ToArray<VisitData>();
            bool runningOnSingleServer = this.GetIsRunningOnSingleServer();
            int num = 0;
            foreach (VisitData visitData1 in array)
            {
                IMongoQuery mongoQuery = Query.And(Query<VisitData>.EQ<Guid>((Expression<Func<VisitData, Guid>>)(x => x.ContactId), visitData1.ContactId), Query<VisitData>.EQ<Guid>((Expression<Func<VisitData, Guid>>)(x => x.InteractionId), visitData1.InteractionId));
                ++num;
                List<IMongoUpdate> mongoUpdateList = new List<IMongoUpdate>();
                if (visitData1.ContactId != survivingContactId)
                {
                    if (!runningOnSingleServer)
                    {
                        int survivingContactIndex = num;
                        this.UpdateShardedVisit(mongoQuery, (Action<VisitData>)(visitData =>
                        {
                            visitData.ContactId = survivingContactId;
                            visitData.ContactVisitIndex = survivingContactIndex;
                        }));
                        continue;
                    }
                    mongoUpdateList.Add(Update<VisitData>.Set<Guid>(x => x.ContactId, survivingContactId));
        }
                if (visitData1.ContactVisitIndex != num)
                {
                    mongoUpdateList.Add(Update<VisitData>.Set<int>(x => x.ContactVisitIndex, num));
        }
                if (mongoUpdateList.Count > 0)
                {
                    IMongoUpdate update = (IMongoUpdate)Update<VisitData>.Combine((IEnumerable<IMongoUpdate>)mongoUpdateList);
                    this.UpdateInteraction(mongoQuery, update);
                }
            }
        }

        public void RenumberInteractions(Guid contactId)
        {
            IMongoQuery query = Query.And(new IMongoQuery[]
            {
                Query.EQ("ContactId", BsonValue.Create(contactId)),
                Query.EQ("_t", BsonValue.Create(typeof(VisitData).Name))
            });
            MongoCursor<VisitData> mongoCursor = this.interactions.FindAs<VisitData>(query);
            bool flag = mongoCursor == null;
            if (flag)
            {
                throw new DatabaseNotAvailableException();
            }
            VisitData[] array = mongoCursor.SetSortOrder(new string[]
            {
                "StartDateTime",
                "_id"
            }).SetFields(Fields.Include(new string[]
            {
                "ContactId",
                "ContactVisitIndex"
            })).ToArray<VisitData>();
            for (int i = 0; i < array.Length; i++)
            {
                VisitData visitData = array[i];
                IMongoQuery existingInteraction = Query.And(new IMongoQuery[]
                {
                    Query<VisitData>.EQ<Guid>((VisitData x) => x.ContactId, visitData.ContactId),
                    Query<VisitData>.EQ<Guid>((VisitData x) => x.InteractionId, visitData.InteractionId)
                });
                int num = i + 1;
                bool flag2 = visitData.ContactVisitIndex != num;
                if (flag2)
                {
                    IMongoUpdate update = Update<VisitData>.Set<int>(x => x.ContactVisitIndex, num);
                    this.UpdateInteraction(existingInteraction, update);
                }
            }
        }


        private void UpdateInteraction(IMongoQuery existingInteraction, IMongoUpdate update)
        {
            MongoUpdateOptions options = new MongoUpdateOptions
            {
                Flags = UpdateFlags.None,
                WriteConcern = WriteConcern.Acknowledged
            };
            this.interactions.Update(existingInteraction, update, options);
        }

        private void UpdateShardedVisit(IMongoQuery existingVisit, Action<VisitData> updateVisit)
        {
            VisitData data = this.interactions.FindOneAs<VisitData>(existingVisit);
            if (data == null)
            {
                throw new DatabaseNotAvailableException();
            }
            updateVisit(data);
            try
            {
                this.interactions.Insert<VisitData>(data);
                this.interactions.Remove(existingVisit, RemoveFlags.Single, WriteConcern.Unacknowledged);
            }
            catch (MongoWriteConcernException exception)
            {
                int? code = exception.Code;
                int num = 11000;
                if ((code.GetValueOrDefault() == num) ? !code.HasValue : true)
                {
                    throw;
                }
                this.interactions.Remove(existingVisit, RemoveFlags.Single, WriteConcern.Acknowledged);
                this.interactions.Insert<VisitData>(data);
            }
        }
    }
}
