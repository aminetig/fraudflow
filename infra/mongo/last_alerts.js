// Affiche les 10 dernières alertes de fraude (lancement : make alerts)
db.getSiblingDB("admin").auth(
  process.env.MONGO_INITDB_ROOT_USERNAME,
  process.env.MONGO_INITDB_ROOT_PASSWORD
);
const alerts = db.getSiblingDB("fraudflow").alerts;

print("Alertes par règle :");
alerts
  .aggregate([{ $group: { _id: "$rule", alertes: { $sum: 1 } } }, { $sort: { _id: 1 } }])
  .forEach((r) => print(`  ${r._id.padEnd(22)} ${r.alertes}`));

print("\n10 dernières alertes :");
alerts
  .find({}, { _id: 0, rule: 1, level: 1, score: 1, card_last4: 1, last_event_time: 1, evidence: 1 })
  .sort({ updated_at: -1 })
  .limit(10)
  .forEach((a) =>
    print(
      `  ${a.last_event_time.toISOString()}  ${a.rule.padEnd(22)} ${a.level.padEnd(6)} ` +
        `score ${a.score}  carte ****${a.card_last4}  preuves ${JSON.stringify(a.evidence)}`
    )
  );
