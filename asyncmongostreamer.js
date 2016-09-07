#!/usr/bin/env node
'use strict';

const DEFAULT_BATCH_SIZE	=	8;
const DEFAULT_TIMEOUT_MS	=	10;
const DEFAULT_MS_TO_DEFER	=	250;

const mongoClient = require('mongodb').MongoClient;
// const co = require('co');
	
class AsyncMongoStreamer {
	constructor(opts) {
		this.url = opts.url;
		this.collection = opts.collection;
		this.batchSize = opts.batchSize || DEFAULT_BATCH_SIZE;
		this.maxMsToDefer = opts.maxMsToDefer || DEFAULT_MS_TO_DEFER;
		this.timeStamped = opts.timeStamped || false;
		this.db = null;
		this.recordColl = null;
		this.latestCommitAt = 0;
		this.recordQueue = [];
		this.isStopped = false;
	}
	
	commit(record) {
		let date = new Date();
		this.latestCommitAt = date.getTime();
		if (this.timeStamped) record['[ts@ams]'] = date;
		this.recordQueue.push(record);
	}
	
	batchCommit(records) {
		let date = new Date();
		this.latestCommitAt = date.getTime();
		if (this.timeStamped) for (let record of records) record['[ts@ams]'] = date;
		this.recordQueue.splice(this.recordQueue.length, 0, records);
	}
	
	_flushToDb() { //flush是清除,清洗的意思
		let self = this;
		if (this.isStopped) {
			if (this.recordQueue.length > 0 && this.db && this.collection) {
				this.recordColl.insertMany(this.recordQueue, null, function(err, res) {
					self.recordQueue = [];
					self.db.close(true, function(err, res) {
						self.collection = null;
						self.db = null;
					});
				});
			}
			/*
			co(function*() {
				if (self.recordQueue.length > 0 && self.db) {
					yield self.recordColl.insertMany(self.recordQueue);
					self.recordQueue = [];
				}
				yield self.db.close();
				self.db = null;
			})
			.catch(err => { console.error(err.stack); });
			*/
			return;
		}
		if (this.db && this.recordColl) {
			let now = new Date().getTime();
			if (this.recordQueue.length >= this.batchSize) {
				let recordsToSave = this.recordQueue.splice(0, this.batchSize);
				this.recordColl.insertMany(recordsToSave, null, function(err, res) {
				});
			} else if (this.latestCommitAt > 0 && now - this.latestCommitAt >= this.maxMsToDefer && this.recordQueue.length > 0) {
				let recordsToSave = this.recordQueue.splice(0, this.recordQueue.length);
				this.recordColl.insertMany(recordsToSave, null, function(err, res) {
				});
				this.latestCommitAt = 0;
			}
			/*
			co(function*() {
				while (self.recordQueue.length > 0 && self.db) {
					let record = self.recordQueue.shift();
					yield self.recordColl.save(record);
				}
			})
			.catch(err => { console.error(err.stack); });
			*/
		}
		
		setTimeout(function() {
			self._flushToDb();
		}, DEFAULT_TIMEOUT_MS);
	}
	
	start() {
		let self = this;
		mongoClient.connect(this.url, null, function(err, db) {
			self.db = db;
			db.collection(self.collection, null, function(err, coll) {
				self.recordColl = coll;
			});
		});
		/*
		co(function*() {
			self.db = yield mongoClient.connect(self.url);
			self.recordColl = self.db.collection(self.collection);
			self._flushToDb();
		})
		.catch(err => { console.error(err.stack); });
		*/
		this._flushToDb();
		return this;
	}
	
	stop() {
		this.isStopped = true;
	}
}

module.exports = AsyncMongoStreamer;











