/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jctools.jmh.latency;

import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
import org.jctools.queues.MpscUnboundedXaddArrayQueue;
import org.openjdk.jmh.annotations.Param;

public class XaddMpqBurstCost extends MpqBurstCost
{

    @Param(value = {"132000"})
    int chunkSize;

    @Param( {"2"})
    int pooledChunks;

    @Override
    MessagePassingQueue<Event> createQueue()
    {
        if (qType.equals(MpscUnboundedXaddArrayQueue.class.getSimpleName()))
        {
            return new MpscUnboundedXaddArrayQueue<MpqBurstCost.Event>(64, 2);
        }
        if (qType.equals(MpmcUnboundedXaddArrayQueue.class.getSimpleName()))
        {
            return new MpmcUnboundedXaddArrayQueue<MpqBurstCost.Event>(64, 2);
        }
        return super.createQueue();
    }

    @Override
    MessagePassingQueue<Event> buildQueue()
    {
        if (qType.equals(MpscUnboundedXaddArrayQueue.class.getSimpleName()))
        {
            return new MpscUnboundedXaddArrayQueue<Event>(chunkSize, pooledChunks);
        }
        if (qType.equals(MpmcUnboundedXaddArrayQueue.class.getSimpleName()))
        {
            return new MpmcUnboundedXaddArrayQueue<Event>(chunkSize, pooledChunks);
        }
        return super.buildQueue();
    }
}
