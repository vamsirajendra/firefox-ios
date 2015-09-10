/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Shared
import Sync
import XCTest

func identity<T>(x: T) -> T {
    return x
}

class MockBackoffStorage: BackoffStorage {
    var serverBackoffUntilLocalTimestamp: Timestamp?

    func clearServerBackoff() {
        serverBackoffUntilLocalTimestamp = nil
    }

    func isInBackoff(now: Timestamp) -> Timestamp? {
        return nil
    }
}

internal func getEncrypter() -> RecordEncrypter<CleartextPayloadJSON> {
    let keyBundle = KeyBundle.random()
    let dec: JSON -> CleartextPayloadJSON = { CleartextPayloadJSON($0) }
    let enc: CleartextPayloadJSON -> JSON = { $0 }
    let encoder = RecordEncoder<CleartextPayloadJSON>(decode: dec, encode: enc)
    return RecordEncrypter(bundle: keyBundle, encoder: encoder)
}

class DownloadTests: XCTestCase {
    func testBasicDownload() {
        let server = MockSyncServer(username: "1234567")
        server.storeRecords([], inCollection: "bookmarks")
        server.start()
        guard let url = server.baseURL.asURL else {
            XCTFail("Couldn't get URL.")
            return
        }

        let authorizer: Authorizer = identity
        let queue = dispatch_get_main_queue()
        print("URL: \(url)")
        let storageClient = Sync15StorageClient(serverURI: url, authorizer: authorizer, workQueue: queue, resultQueue: queue, backoff: MockBackoffStorage())
        let bookmarksClient = storageClient.clientForCollection("bookmarks", encrypter: getEncrypter())

        let deferred = bookmarksClient.getSince(0)
        let result = deferred.value
        XCTAssertTrue(result.isSuccess)
        guard let response = result.successValue else {
            XCTFail("Request should not have failed.")
            return
        }
        XCTAssertEqual(response.metadata.status, 200)
    }
}