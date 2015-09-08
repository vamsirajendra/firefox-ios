/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Shared
import Storage
import XCGLogger

private let log = Logger.syncLogger
private let BookmarksStorageVersion = 2

/**
 * This is like a synchronizer, but it downloads records bit by bit, eventually
 * notifying that the local storage is up to date with the server contents.
 *
 * Because batches might be separated over time, it's possible for the server
 * state to change between calls. These state changes might include:
 *
 * 1. New changes arriving. This is fairly routine, but it's worth noting that
 *    changes might affect existing records that have been batched!
 * 2. Wipes. The collection (or the server as a whole) might be deleted. This
 *    should be accompanied by a change in syncID in meta/global; it's the caller's
 *    responsibility to detect this.
 * 3. A storage format change. This should be unfathomably rare, but if it happens
 *    we must also be prepared to discard our existing batched data.
 * 4. TTL expiry. We need to do better about TTL handling in general, but here
 *    we might find that a downloaded record is no longer live by the time we
 *    come to apply it! This doesn't apply to bookmark records, so we will ignore
 *    it for the moment.
 *
 * Batch downloading without continuation tokens is achieved as follows:
 *
 * * A minimum timestamp is established. This starts as zero.
 * * A fetch is issued against the server for records changed since that timestamp,
 *   ordered by modified time ascending, and limited to the batch size.
 * * If the batch is complete, we flush it to storage and advance the minimum
 *   timestamp to just before the newest record in the batch. This ensures that
 *   a divided set of records with the same modified times will be downloaded
 *   entirely so long as the set is never larger than the batch size.
 * * Iterate until we determine that there are no new records to fetch.
 *
 * Batch downloading with continuation tokens is much easier:
 *
 * * A minimum timestamp is established.
 * * Make a request with limit=N.
 * * Look for an X-Weave-Next-Offset header. Supply that in the next request.
 *   Also supply X-If-Unmodified-Since to avoid missed modifications.
 *
 * We do the latter, because we only support Sync 1.5. The use of the offset
 * allows us to efficiently process batches, particularly those that contain
 * large sets of records with the same timestamp. We still maintain the last
 * modified timestamp to allow for resuming a batch in the case of a conflicting
 * write, detected via X-I-U-S.
 */
class BatchingDownloader<T: CleartextPayloadJSON> {
    let delegate: DownloaderDelegate
    let client: Sync15CollectionClient<T>
    let collection: String
    let prefs: Prefs

    var nextOffset: String? {
        get {
            return self.prefs.stringForKey("nextOffset")
        }
        set (value) {
            if let value = value {
                self.prefs.setString(value, forKey: "nextOffset")
            } else {
                self.prefs.removeObjectForKey("nextOffset")
            }
        }
    }

    var baseTimestamp: Timestamp {
        get {
            return self.prefs.timestampForKey("baseTimestamp") ?? 0
        }
        set (value) {
            self.prefs.setTimestamp(value ?? 0, forKey: "baseTimestamp")
        }
    }

    var lastModified: Timestamp {
        get {
            return self.prefs.timestampForKey("lastModified") ?? 0
        }
        set (value) {
            self.prefs.setTimestamp(value ?? 0, forKey: "lastModified")
        }
    }

    /**
     * Call this when a significant structural server change has been detected.
     */
    func reset() -> Success {
        // TODO
        return succeed()
    }

    // TODO: more useful return value, or a delegate.
    func go(info: InfoCollections, limit: Int) {
        guard let modified = info.modified(self.collection) else {
            log.debug("No server modified time for collection \(self.collection).")
            delegate.onNoNewData()
            return
        }

        if modified == self.lastModified {
            log.debug("No more data to batch-download.")
            delegate.onNoNewData()
            return
        }

        self.downloadNextBatchWithLimit(limit, advancingOnCompletionTo: modified)
    }

    func downloadNextBatchWithLimit(limit: Int, advancingOnCompletionTo: Timestamp) {
        func handleFailure(err: MaybeErrorType) {
            guard let badRequest = err as? BadRequestError<[Record<T>]> where badRequest.response.metadata.status == 412 else {
                // Just pass through the failure.
                delegate.onFailure(err)
                return
            }

            // Conflict. Start again.
            log.warning("Server contents changed during offset-based batching. Stepping back.")
            self.nextOffset = nil
            delegate.onInterrupted()
        }

        func handleSuccess(response: StorageResponse<[Record<T>]>) {
            // Shift to the next offset. This might be nil, in which case… fine!
            let offset = response.metadata.nextOffset
            self.nextOffset = offset

            // If there are records, advance to just before the timestamp of the last.
            // If our next fetch with X-Weave-Next-Offset fails, at least we'll start here.
            if let newBase = response.value.last?.modified {
                self.baseTimestamp = newBase - 1
            }

            // Process the incoming records.
            delegate.applyBatch(response.value)

            if offset == nil {
                delegate.onComplete()
            } else {
                delegate.onIncomplete()
            }
        }

        let fetch = self.client.getSince(self.baseTimestamp, sort: SortOption.newest, limit: limit, offset: self.nextOffset)
        return fetch.upon { result in
            if let response = result.successValue {
                handleSuccess(response)
            } else {
                handleFailure(result.failureValue!)
            }
        }
    }

    init(collectionClient: Sync15CollectionClient<T>, basePrefs: Prefs, collection: String, delegate: DownloaderDelegate) {
        self.delegate = delegate
        self.client = collectionClient
        self.collection = collection
        let branchName = "downloader." + collection + "."
        self.prefs = basePrefs.branch(branchName)

        log.info("Downloader configured with prefs '\(branchName)'.")
    }
}

public protocol DownloaderDelegate {
    func applyBatch<T: CleartextPayloadJSON>(records: [Record<T>]) -> Success
    func onComplete()                         // We're done. applyBatch was called, and there are no more records.
    func onIncomplete()                       // applyBatch was called, and we think there are more records.
    func onNoNewData()                        // There were no records.
    func onInterrupted()                      // We got a 412 conflict when fetching the next batch.
    func onFailure(err: MaybeErrorType)       // We got any other error.
}