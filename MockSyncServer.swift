/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

import Foundation
import Shared
import Sync
import XCTest

class MockSyncServer {
    let server = GCDWebServer()
    let username: String

    var collections: [String: [String: EnvelopeJSON]] = [:]
    var baseURL: String!

    init(username: String) {
        self.username = username
    }

    func storeRecords(records: [EnvelopeJSON], inCollection collection: String) {
        var out = self.collections[collection] ?? [:]
        records.forEach { out[$0.id] = $0 }
        self.collections[collection] = out
    }

    func start() {
        let basePath = "/1.5/\(self.username)/"
        let storagePath = "\(basePath)storage/"

        let match: GCDWebServerMatchBlock = { method, url, headers, path, query -> GCDWebServerRequest! in
            guard method == "GET" && path.startsWith(storagePath) else {
                return nil
            }
            return GCDWebServerRequest(method: method, url: url, headers: headers, path: path, query: query)
        }

        server.addHandlerWithMatchBlock(match) { (request) -> GCDWebServerResponse! in
            // Return an array of values.
            return GCDWebServerDataResponse(JSONObject: [], contentType: "application/json")
        }

        if server.startWithPort(0, bonjourName: nil) == false {
            XCTFail("Can't start the GCDWebServer.")
        }

        baseURL = "http://localhost:\(server.port)\(basePath)"
    }
}