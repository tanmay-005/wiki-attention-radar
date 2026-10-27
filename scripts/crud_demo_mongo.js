// Step 5.x: MongoDB queries, CRUD, aggregation, data types and an index plan.
// Runs after the serving load (bash scripts/run_s6.sh), from scripts/mongo_demo.sh.
// The CRUD part uses its own document (_id "evt_demo_1"), so the real data is not touched.
const r = db.getSiblingDB("radar");
const show = (title, x) => { print("\n== " + title); printjson(x); };

show("1. documents per collection", {
  events: r.events.countDocuments(), pages: r.pages.countDocuments(),
  wikidata_cache: r.wikidata_cache.countDocuments(), meta: r.meta.findOne({ _id: "serving" }) });

show("2. the top global event (curve, articles and countries shortened to 3 entries)",
  r.events.find({ scopes: "G", suspicious: false }, { curve: { $slice: 3 }, articles: { $slice: 3 }, countries: { $slice: 3 } })
    .sort({ score: -1 }).limit(1).toArray()[0]);

show("3. top 5 events read by people in India",
  r.events.find({ scopes: "R:IN" }, { _id: 0, title: 1, category: 1, score: 1, ratio: 1 }).sort({ score: -1 }).limit(5).toArray());

const plan = r.events.find({ scopes: "R:IN" }).sort({ score: -1 }).limit(5).explain("executionStats");
const stage = (p) => p.inputStage ? stage(p.inputStage) : p;
show("4. how query 3 runs (index on {scopes: 1, score: -1})", {
  input_stage: stage(plan.queryPlanner.winningPlan).stage,
  index: stage(plan.queryPlanner.winningPlan).indexName,
  docs_examined: plan.executionStats.totalDocsExamined,
  docs_returned: plan.executionStats.nReturned,
  ms: plan.executionStats.executionTimeMillis });

show("5. events about a region (location.region = IN-RJ, Rajasthan)",
  r.events.find({ "location.region": "IN-RJ" }, { _id: 0, title: 1, category: 1, "location.region_name": 1, "location.city_name": 1 }).limit(5).toArray());

show("6. events that contain a given Wikidata item in their articles (Q85518537, Carlos Alcaraz)",
  r.events.find({ "articles.qid": "Q85518537" }, { _id: 1, title: 1 }).limit(5).toArray());

show("7. aggregation: events, average jump and best score per category",
  r.events.aggregate([
    { $group: { _id: "$category", events: { $sum: 1 }, avg_ratio: { $avg: "$ratio" }, best: { $max: "$score" } } },
    { $sort: { events: -1 } },
    { $project: { events: 1, avg_ratio: { $round: ["$avg_ratio", 1] }, best: { $round: ["$best", 0] } } }]).toArray());

show("8. aggregation with $unwind: countries with the most spiking events",
  r.events.aggregate([
    { $unwind: "$countries" },
    { $group: { _id: { code: "$countries.code", name: "$countries.name" }, events: { $sum: 1 } } },
    { $sort: { events: -1 } }, { $limit: 10 }]).toArray());

show("9. BSON types stored in one document",
  r.events.aggregate([{ $limit: 1 }, { $project: { _id: 0,
    _id_type: { $type: "$_id" }, first_detected: { $type: "$first_detected" }, score: { $type: "$score" },
    peak_views: { $type: "$peak_views" }, langs: { $type: "$langs" }, location: { $type: "$location" },
    suspicious: { $type: "$suspicious" }, members: { $type: "$members" } } }]).toArray());

// 10. CRUD on a demo document
r.events.deleteOne({ _id: "evt_demo_1" });
show("10a. insertOne", r.events.insertOne({ _id: "evt_demo_1", title: "Demo event", category: "Other",
  first_detected: new Date("2024-06-04T03:00:00Z"), score: 123.4, ratio: 12.5, peak_views: NumberLong("5000"),
  langs: ["en"], location: { country: "IN", region: "IN-RJ" }, scopes: ["DEMO"], suspicious: false }));
show("10b. find", r.events.findOne({ _id: "evt_demo_1" }));
show("10c. updateOne ($set, $push, $inc)", r.events.updateOne({ _id: "evt_demo_1" },
  { $set: { title: "Demo event (edited)" }, $push: { langs: "hi" }, $inc: { score: 10 } }));
show("10d. find after the update", r.events.findOne({ _id: "evt_demo_1" }, { title: 1, langs: 1, score: 1 }));
show("10e. deleteOne", r.events.deleteOne({ _id: "evt_demo_1" }));
show("10f. count after the delete", r.events.countDocuments({ _id: "evt_demo_1" }));

show("11. indexes on events", r.events.getIndexes().map(i => i.name));
