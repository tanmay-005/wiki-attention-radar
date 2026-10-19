// Wikipedia Attention Radar: MongoDB collections + indexes (see docs/03_DATA_MODEL.md §5)
// Run: docker compose exec mongo mongosh --quiet /workspace/scripts/init_mongo.js
const rdb = db.getSiblingDB("radar");

for (const name of ["events", "pages", "validation"]) {
  if (!rdb.getCollectionNames().includes(name)) {
    rdb.createCollection(name);
  }
}

rdb.events.createIndex({ category: 1, first_detected: -1 });
rdb.events.createIndex({ "location.country": 1, "location.region": 1 });
rdb.events.createIndex({ "articles.qid": 1 });
rdb.events.createIndex({ scopes: 1, score: -1 });
rdb.pages.createIndex({ qid: 1 });

print("collections: " + JSON.stringify(rdb.getCollectionNames().sort()));
print("events indexes: " + rdb.events.getIndexes().length);
