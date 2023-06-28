package testdata

type testInput struct {
	Name  string
	Input string
}

var (
	BadInputs = []testInput{
		{
			Name: "RepeatedEntriesField",
			Input: `
{
	"entries": [
	{
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
			},
		}
	},
	],
	entries: []
}`,
		},
		{
			Name: "MissingObjectStart",
			Input: `
	"entries": [
	{
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
			}
		}
	}
	]
}`,
		},
		{
			Name: "MissingObjectEnd",
			Input: `
{
	"entries": [
	{
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
			}
		}
	}
	]`,
		},
		{
			Name: "MissingArrayStart",
			Input: `
{
	"entries":
	{
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
			}
		}
	}
	]
}`,
		},
		{
			Name: "MissingArrayEnd",
			Input: `
{
	"entries": [
	{
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
			}
		}
	}
}`,
		},
		{
			Name: "MissingInnerObjectStart",
			Input: `
{
	"entries": [
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
			}
		}
	},
	]
}`,
		},
		{
			Name: "BadInnerObject",
			Input: `
{
	"entries": [
	{
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
		}
	}
	]
}`,
		},
		{
			Name: "MissingInnerObjectEnd",
			Input: `
{
	"entries": [
	{
		"id": "25905b6f222a153561543baea0a67043",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-16T20:46:59.70714Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-16T20:46:55.76843Z",
			"endTime": "2023-03-16T20:46:59.707064Z",
			"stats": {
				"totalSize": 536927459,
				"excludedTotalSize": 0,
				"fileCount": 18,
				"cachedFiles": 0,
				"nonCachedFiles": 18,
				"dirCount": 14,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k74647859396c88127696f426b4c79088",
				"summ": {
					"size": 536927459,
					"files": 18,
					"symlinks": 0,
					"dirs": 14,
					"maxTime": "2023-03-16T20:46:56.187394Z",
					"numFailed": 0
				}
			}
		}
	]
}`,
		},
	}
)

const (
	GoodManifests = `
{
	"entries": [
	{
		"id": "2e14cba9427c57223dd768bd1ddf694c",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"tag": "value",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:08:32.962808Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:08:29.674573Z",
			"endTime": "2023-03-17T01:08:32.962614Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 143,
				"cachedFiles": 0,
				"nonCachedFiles": 143,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "kfe00a91781912fc352edca26571a5f83",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:08:29.677079Z",
					"numFailed": 0
				}
			},
			"tags": {
				"tag": "value"
			}
		}
	},
	{
		"id": "2c54893efd80bcda7102f622da5c63ee",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:11:34.506121Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:11:22.34148Z",
			"endTime": "2023-03-17T01:11:34.505952Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 2,
				"cachedFiles": 141,
				"nonCachedFiles": 2,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k4f1a9e8049091615cbe4ad93507680f3",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:11:22.725375Z",
					"numFailed": 0
				}
			}
		}
	}
	]
}
`
	IgnoredField = `
{
	"entries": [
	{
		"id": "2e14cba9427c57223dd768bd1ddf694c",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"tag": "value",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:08:32.962808Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:08:29.674573Z",
			"endTime": "2023-03-17T01:08:32.962614Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 143,
				"cachedFiles": 0,
				"nonCachedFiles": 143,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "kfe00a91781912fc352edca26571a5f83",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:08:29.677079Z",
					"numFailed": 0
				}
			},
			"tags": {
				"tag": "value"
			}
		}
	},
	{
		"id": "2c54893efd80bcda7102f622da5c63ee",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:11:34.506121Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:11:22.34148Z",
			"endTime": "2023-03-17T01:11:34.505952Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 2,
				"cachedFiles": 141,
				"nonCachedFiles": 2,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k4f1a9e8049091615cbe4ad93507680f3",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:11:22.725375Z",
					"numFailed": 0
				}
			}
		}
	}
	],
	"ignored": "hello world"
}`
	ExtraInputAtEnd = `
{
	"entries": [
	{
		"id": "2e14cba9427c57223dd768bd1ddf694c",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"tag": "value",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:08:32.962808Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:08:29.674573Z",
			"endTime": "2023-03-17T01:08:32.962614Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 143,
				"cachedFiles": 0,
				"nonCachedFiles": 143,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "kfe00a91781912fc352edca26571a5f83",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:08:29.677079Z",
					"numFailed": 0
				}
			},
			"tags": {
				"tag": "value"
			}
		}
	},
	{
		"id": "2c54893efd80bcda7102f622da5c63ee",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:11:34.506121Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:11:22.34148Z",
			"endTime": "2023-03-17T01:11:34.505952Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 2,
				"cachedFiles": 141,
				"nonCachedFiles": 2,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k4f1a9e8049091615cbe4ad93507680f3",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:11:22.725375Z",
					"numFailed": 0
				}
			}
		}
	}
	]
}abcdefg`
	CaseInsensitive = `
{
	"Entries": [
	{
		"id": "2e14cba9427c57223dd768bd1ddf694c",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"tag": "value",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:08:32.962808Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:08:29.674573Z",
			"endTime": "2023-03-17T01:08:32.962614Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 143,
				"cachedFiles": 0,
				"nonCachedFiles": 143,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "kfe00a91781912fc352edca26571a5f83",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:08:29.677079Z",
					"numFailed": 0
				}
			},
			"tags": {
				"tag": "value"
			}
		}
	},
	{
		"id": "2c54893efd80bcda7102f622da5c63ee",
		"labels": {
			"hostname": "host-name",
			"path": "/root/tmp/test",
			"type": "snapshot",
			"username": "user-name"
		},
		"modified": "2023-03-17T01:11:34.506121Z",
		"data": {
			"id": "",
			"source": {
				"host": "host-name",
				"userName": "user-name",
				"path": "/root/tmp/test"
			},
			"description": "",
			"startTime": "2023-03-17T01:11:22.34148Z",
			"endTime": "2023-03-17T01:11:34.505952Z",
			"stats": {
				"totalSize": 427221,
				"excludedTotalSize": 0,
				"fileCount": 2,
				"cachedFiles": 141,
				"nonCachedFiles": 2,
				"dirCount": 10,
				"excludedFileCount": 0,
				"excludedDirCount": 0,
				"ignoredErrorCount": 0,
				"errorCount": 0
			},
			"rootEntry": {
				"name": "test",
				"type": "d",
				"mode": "0777",
				"mtime": "1754-08-30T22:43:41.128654848Z",
				"obj": "k4f1a9e8049091615cbe4ad93507680f3",
				"summ": {
					"size": 427221,
					"files": 143,
					"symlinks": 0,
					"dirs": 10,
					"maxTime": "2023-03-17T01:11:22.725375Z",
					"numFailed": 0
				}
			}
		}
	}
	]
}`
)
