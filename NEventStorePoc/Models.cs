using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CSharp.RuntimeBinder;

namespace NEventStorePoc
{
    public class Event
    {
        public static Event<T> Create<T>(Guid streamId, T data)
        {
            return Create(Guid.NewGuid(), streamId, data);
        }

        public static Event<T> Create<T>(Guid eventId, Guid streamId, T data)
        {
            return new Event<T>(eventId, streamId, data);
        }
    }

    public class Event<T>
    {
        public Event(Guid eventId, Guid streamId, T data)
        {
            EventId = eventId;
            StreamId = streamId;
            Data = data;
        }

        public Guid StreamId { get; }
        public Guid EventId { get; }
        public T Data { get; }
    }

    public class SomethingHappened
    {
        public SomethingHappened(string something)
        {
            Something = something;
        }

        public string Something { get; }
    }

    public class SomeSnapshot
    {
        public SomeSnapshot() { }
        public SomeSnapshot(IEnumerable<object> events)
        {
            foreach (var @event in events)
            {
                try
                {
                    Apply((dynamic)@event);
                }
                catch (RuntimeBinderException)
                {
                    //unhandled event, skip
                }
            }
        }

        public ICollection<string> State { get; set; } = new List<string>();

        public Guid Id { get; set; }

        public int Version { get; set; }

        private void Apply(SomethingHappened @event)
        {
            State = State.Union(new[] { @event.Something }).ToList();
        }
    }

}
