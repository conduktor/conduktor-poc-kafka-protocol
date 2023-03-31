/*
 * Copyright 2023 Conduktor, Inc
 *
 * Licensed under the Conduktor Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * https://www.conduktor.io/conduktor-community-license-agreement-v1.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.conduktor.example.loggerinterceptor;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.record.*;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordUtils {

    public static BaseRecords addHeaderToRecords(BaseRecords records, String key, String value) {
        var batchMap = new LinkedHashMap<RecordBatch, List<RecordAndOffset>>();
        AtomicInteger batchesTotalSize = new AtomicInteger();
        ((MemoryRecords) records).batches().forEach(batch -> {
            batchesTotalSize.addAndGet(batch.sizeInBytes());
            var newRecords = new LinkedList<RecordAndOffset>();
            batch.forEach(record -> {

                var newRecord = new SimpleRecord(
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        addHeader(record.headers(), key, value)
                        );
                newRecords.add(new RecordAndOffset(newRecord, record.offset()));
            });
            batchMap.put(batch,newRecords);
        });
        var recordsOnly = batchMap.values()
                .stream()
                .flatMap(recordAndOffsets -> recordAndOffsets.stream()
                        .map(RecordAndOffset::record))
                .toList();
        int sizeEstimate = AbstractRecords.estimateSizeInBytes(RecordBatch.CURRENT_MAGIC_VALUE, CompressionType.NONE, recordsOnly);
        var buf = ByteBuffer.allocate(sizeEstimate + batchesTotalSize.get());
        for (var entry : batchMap.entrySet()) {
            writeRecords(buf, entry.getValue(), entry.getKey());
        }
        buf.flip();
        return MemoryRecords.readableRecords(buf);

    }

    private static Header[] addHeader(Header[] headers, String key, String value) {
        var listHeaders = new ArrayList<>(List.of(headers));
        listHeaders.add(new RecordHeader(key, value.getBytes()));
        return listHeaders.toArray(new Header[0]);
    }

    private static void writeRecords(ByteBuffer buf, List<RecordAndOffset> recordAndOffsets, RecordBatch batch) {
        long baseOffset = recordAndOffsets.stream()
                .map(RecordAndOffset::offset)
                .min(Comparator.comparingLong(e -> e)).orElse(0L);
        try (var builder = new MemoryRecordsBuilder(
                buf,
                batch.magic(),
                batch.compressionType(),
                batch.timestampType(),
                baseOffset,
                -1L,
                batch.producerId(),
                batch.producerEpoch(),
                batch.baseSequence(),
                batch.isTransactional(),
                batch.isControlBatch(),
                batch.partitionLeaderEpoch(),
                buf.capacity()
        )) {

            for (var recordAndOffset : recordAndOffsets) {
                if (batch.isControlBatch()) {
                    builder.appendControlRecordWithOffset(recordAndOffset.offset(), recordAndOffset.record());
                } else {
                    builder.appendWithOffset(recordAndOffset.offset(), recordAndOffset.record());
                }
            }
            builder.build();
        }
    }

    private static record RecordAndOffset(SimpleRecord record, Long offset) {}
}
