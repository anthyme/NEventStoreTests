using System;
using System.Data.SqlClient;
using System.Linq;
using Dapper;
using NEventStore;
using NEventStore.Persistence.Sql;
using NEventStore.Persistence.Sql.SqlDialects;
using Shouldly;
using Xunit;

namespace NEventStorePoc
{
    public class Given_a_Memory_EventStore
    {
        public static IStoreEvents CreateMemoryConnection()
        {
            return Wireup.Init()
                        .UsingInMemoryPersistence()
                        .InitializeStorageEngine()
                        .Build();
        }

        [Fact]
        public void When_an_event_is_published_then_it_is_stored_in_memory_and_returned_in_the_stream()
        {
            var streamId = Guid.NewGuid();
            var event1 = Event.Create(streamId, new SomethingHappened("data1"));

            using (var store = CreateMemoryConnection())
            {
                using (var stream = store.OpenStream(streamId, 0))
                {
                    stream.Add(new EventMessage { Body = event1.Data });
                    stream.CommitChanges(event1.EventId);
                }

                using (var stream = store.OpenStream(streamId, 0))
                {
                    stream.CommittedEvents.Count.ShouldBe(1);
                }
            }
        }

        [Fact]
        public void When_two_events_are_published_simutanously_then_the_second_commit_fail()
        {
            var streamId = Guid.NewGuid();
            var event1 = Event.Create(streamId, new SomethingHappened("data1"));
            var event2 = Event.Create(streamId, new SomethingHappened("data2"));

            using (var store = CreateMemoryConnection())
            using (var stream = store.OpenStream(streamId, 0))
            using (var stream2 = store.OpenStream(streamId, 0))
            {
                stream.Add(new EventMessage { Body = event1.Data });
                stream.CommitChanges(event1.EventId);

                stream2.Add(new EventMessage { Body = event2.Data });
                Assert.Throws<ConcurrencyException>(() => stream2.CommitChanges(event2.EventId));
            }
        }
    }

    public class Given_a_Sql_EventStore
    {
        public const string ConnectionString =
                "Server=(localdb)\\MSSQLLocalDB;Initial catalog=NEventStore;Integrated Security=true;";
        public static IStoreEvents CreateSqlConnection()
        {
            var config = new ConfigurationConnectionFactory(
                "NEventStorePoc", "system.data.sqlclient", ConnectionString);

            return Wireup.Init()
                .UsingSqlPersistence(config)
                .WithDialect(new MsSqlDialect())
                .InitializeStorageEngine()
                .Build();
        }

        [Fact]
        public void When_an_event_is_published_then_it_is_stored_in_database_and_returned_in_the_stream()
        {
            var streamId = Guid.NewGuid();
            var event1 = Event.Create(streamId, new SomethingHappened("data1"));

            using (var store = CreateSqlConnection())
            using (var stream = store.OpenStream(streamId, 0))
            {
                stream.Add(new EventMessage { Body = event1.Data });
                stream.CommitChanges(event1.EventId);
            }

            using (var connection = new SqlConnection(ConnectionString))
                connection
                .Query("select * from commits where streamIdOriginal = @streamId", new { streamId })
                .Count().ShouldBe(1);

            using (var store = CreateSqlConnection())
            using (var stream = store.OpenStream(streamId, 0))
            {
                stream.CommittedEvents.Count.ShouldBe(1);
            }
        }

        [Fact]
        public void When_two_events_are_published_simutanously_then_the_second_commit_fail()
        {
            var streamId = Guid.NewGuid();
            var event1 = Event.Create(streamId, new SomethingHappened("data1"));
            var event2 = Event.Create(streamId, new SomethingHappened("data2"));

            using (var store = CreateSqlConnection())
            using (var stream = store.OpenStream(streamId, 0))
            using (var stream2 = store.OpenStream(streamId, 0))
            {
                stream.Add(new EventMessage { Body = event1.Data });
                stream.CommitChanges(event1.EventId);

                stream2.Add(new EventMessage { Body = event2.Data });
                Assert.Throws<ConcurrencyException>(() => stream2.CommitChanges(event2.EventId));
            }
        }

        [Fact]
        public void When_two_events_are_published_from_different_connection_then_the_second_commit_fail()
        {
            var streamId = Guid.NewGuid();
            var event1 = Event.Create(streamId, new SomethingHappened("data1"));
            var event2 = Event.Create(streamId, new SomethingHappened("data2"));

            using (var connection1 = CreateSqlConnection())
            using (var connection2 = CreateSqlConnection())
            using (var stream = connection1.OpenStream(streamId, 0))
            using (var stream2 = connection2.OpenStream(streamId, 0))
            {
                stream.Add(new EventMessage { Body = event1.Data });
                stream.CommitChanges(event1.EventId);

                stream2.Add(new EventMessage { Body = event2.Data });
                Assert.Throws<ConcurrencyException>(() => stream2.CommitChanges(event2.EventId));
            }
        }

        [Fact]
        public void When_using_snapshots_only_following_events_should_be_retrieved_when_reopening_the_stream()
        {
            var streamId = Guid.NewGuid();
            var event1 = Event.Create(streamId, new SomethingHappened("data1"));
            var event2 = Event.Create(streamId, new SomethingHappened("data2"));
            var event3 = Event.Create(streamId, new SomethingHappened("data3"));

            using (var connection1 = CreateSqlConnection())
            {
                using (var stream = connection1.OpenStream(streamId, 0))
                {
                    stream.Add(new EventMessage { Body = event1.Data });
                    stream.Add(new EventMessage { Body = event2.Data });
                    stream.Add(new EventMessage { Body = event3.Data });
                    stream.CommitChanges(event1.EventId);

                    var snapshotData = new SomeSnapshot(new[] { event1.Data, event2.Data, event3.Data });
                    var snapshot = new Snapshot(stream.StreamId, stream.StreamRevision, snapshotData);
                    connection1.Advanced.AddSnapshot(snapshot);
                    stream.CommitChanges(Guid.NewGuid());
                    stream.CommittedEvents.Count.ShouldBe(3);
                }

                var snapshot2 = connection1.Advanced.GetSnapshot(streamId, int.MaxValue);
                ((SomeSnapshot)snapshot2.Payload).State.ShouldBe(new[] { "data1", "data2", "data3", });

                using (var stream = connection1.OpenStream(snapshot2, int.MaxValue))
                {
                    stream.CommittedEvents.Count.ShouldBe(0);

                    var event4 = Event.Create(streamId, new SomethingHappened("data4"));
                    var event5 = Event.Create(streamId, new SomethingHappened("data5"));
                    stream.Add(new EventMessage { Body = event4.Data });
                    stream.Add(new EventMessage { Body = event5.Data });
                    stream.CommitChanges(event4.EventId);
                }

                var snapshot3 = connection1.Advanced.GetSnapshot(streamId, int.MaxValue);
                ((SomeSnapshot)snapshot3.Payload).State.ShouldBe(new[] { "data1", "data2", "data3", });

                using (var stream = connection1.OpenStream(snapshot3, int.MaxValue))
                {
                    stream.CommittedEvents.Count.ShouldBe(2);
                    stream.CommittedEvents.Select(x => x.Body).OfType<SomethingHappened>().Select(x => x.Something)
                        .ShouldBe(new[] { "data4", "data5" });
                }
            }
        }
    }
}
