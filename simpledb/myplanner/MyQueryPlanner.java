package simpledb.myplanner;

import java.util.*;
import simpledb.query.*;
import simpledb.record.Schema;
import simpledb.index.query.*;
import simpledb.tx.Transaction;
import simpledb.server.SimpleDB;
import simpledb.parse.QueryData;
import simpledb.metadata.IndexInfo;
import simpledb.planner.QueryPlanner;
import simpledb.multibuffer.MultiBufferProductPlan;

public class MyQueryPlanner implements QueryPlanner {

	public Plan createPlan(QueryData data, Transaction tx) {

		Collection<MyPlanner> tps = new ArrayList<MyPlanner>();

		for (String tableName : data.tables()) {
			tps.add(new MyPlanner(tableName, data.pred(), tx));
		}

		Plan current = null;

		// start with smallest select in tree
		MyPlanner bestTablePlan = null;

		Iterator<MyPlanner> it = tps.iterator();

		while(it.hasNext()) {

			MyPlanner tp = it.next();

			Plan plan = tp.makeSelect();
			if (current == null || plan.recordsOutput() < current.recordsOutput()) {
				bestTablePlan = tp;
				current = plan;
			}
		}

		tps.remove(bestTablePlan);

		while (!tps.isEmpty()) {


			// apply smallest sort of joins
			bestTablePlan = null;
			Plan p = null;

			Iterator<MyPlanner> it = tps.iterator();

			while(it.hasNext()) {

				MyPlanner tp = it.next();

				Plan plan = tp.createJoin(current);

				if (plan != null && (p == null || plan.recordsOutput() < p.recordsOutput())) {

					bestTablePlan = tp;
					p = plan;

				}

			}

			if (p != null) {

				tps.remove(bestTablePlan);

				current = p;

			} else {

				// no join, apply products

				bestTablePlan = null;
				p = null;

				Iterator<MyPlanner> it = tps.iterator();

				while(it.hasNext()) {

					MyPlanner tp = it.next();

					Plan plan = tp.createProduct(current);
					if (p == null || plan.recordsOutput() < p.recordsOutput()) {

						bestTablePlan = tp;
						p = plan;

					}

				}


				if (p != null) {

					tps.remove(bestTablePlan);

					current = p;

				}

			}

		}

		return new ProjectPlan(current, data.fields());

	}

}


class MyPlanner {

	protected Map<String,IndexInfo> _indexes;

	protected Transaction _tx;
	protected Schema _mySchema;
	protected TablePlan _myPlan;
	protected Predicate _myPredicate;

	public MyPlanner(String tableName, Predicate myPredicate, Transaction tx) {

		this._myPredicate = myPredicate;

		this._tx = tx;

		this._myPlan   = new TablePlan(tableName, this._tx);

		this._mySchema = this._myPlan.schema();

		this._indexes  = SimpleDB.mdMgr().getIndexInfo(tableName, this._tx);

	}

	public Plan makeSelect() {

		for (String fieldName : this._indexes.keySet()) {
			Constant constant_ = _myPredicate.equatesWithConstant(fieldName);

			if (constant_ != null) {
				IndexInfo indexInfo = this._indexes.get(fieldName);
				Plan p = new IndexSelectPlan(this._myPlan, indexInfo, constant_, this._tx);

							// select prediction
				Predicate selectPredicate = _myPredicate.selectPredicate(this._mySchema);
				if (selectPredicate != null)
					return new SelectPlan(p, selectPredicate);
				else
					return p;

			}
		}

				// select prediction
		Predicate selectPredicate = _myPredicate.selectPredicate(this._mySchema);

		if (selectPredicate != null)
			return new SelectPlan(this._myPlan, selectPredicate);

		else
			return this._myPlan;
	}

	public Plan createJoin(Plan current) {

		Schema currsch = current.schema();

		Predicate joinPredicate = _myPredicate.joinPredicate(this._mySchema, currsch);
		if (joinPredicate == null)
			return null;

				// index join
		for (String fieldName : this._indexes.keySet()) {

			String outerField = _myPredicate.equatesWithField(fieldName);

			if (outerField != null && currsch.hasField(outerField)) {
				IndexInfo indexInfo = this._indexes.get(fieldName);
				Plan p = new IndexJoinPlan(current, this._myPlan, indexInfo, outerField, this._tx);

							// select prediction
				Predicate selectPredicate = _myPredicate.selectPredicate(this._mySchema);
				if (selectPredicate != null)
					p = new SelectPlan(p, selectPredicate);

							// join predicate
				Predicate joinPredicate = _myPredicate.joinPredicate(currsch, this._mySchema);

				if (joinPredicate != null)

					return new SelectPlan(p, joinPredicate);

				else

					return p;

			}
		}


					// product join
		Plan p = createProduct(current);

					// join predicate
		Predicate joinPredicate = _myPredicate.joinPredicate(currsch, this._mySchema);
		if (joinPredicate != null)
			return new SelectPlan(p, joinPredicate);

		else
			return p;

	}

	public Plan createProduct(Plan current) {

				// select prediction
		Predicate selectPredicate = _myPredicate.selectPredicate(this._mySchema);

		if (selectPredicate != null) {

			return new MultiBufferProductPlan(current, new SelectPlan(this._myPlan, selectPredicate), this._tx);

		} else {

			return new MultiBufferProductPlan(current, this._myPlan, this._tx);

		}

	}

}