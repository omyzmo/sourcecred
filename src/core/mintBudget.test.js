// @flow

import {utcWeek} from "d3-time";
import {type TimestampMs} from "../util/timestamp";
import {NodeAddress} from "./graph";
import {Graph, type NodeAddressT} from "./graph";
import * as WG from "./weightedGraph";
import {empty as emptyWeights} from "./weights";
import {type WeightedGraph as WeightedGraphT} from "./weightedGraph";
import {nodeWeightEvaluator} from "./algorithm/weightEvaluator";

import {
  _anyCommonPrefixes,
  applyBudget,
  _findCurrentBudget,
} from "./mintBudget";

describe("core/mintBudget", () => {
  describe("applyBudget", () => {
    it("errors if there are prefix conflicts", () => {
      const entry1 = {prefix: NodeAddress.empty, periods: []};
      const entry2 = {prefix: NodeAddress.fromParts(["foo"]), periods: []};
      const badBudget = {intervalLength: "WEEKLY", entries: [entry1, entry2]};
      const fail = () => applyBudget(WG.empty(), badBudget);
      expect(fail).toThrow("budget prefix conflict detected");
    });
    it("errors if the intervalLength is not weekly", () => {
      const badBudget = {intervalLength: "DAILY", entries: []};
      // $FlowExpectedError[incompatible-call]
      const fail = () => applyBudget(WG.empty(), badBudget);
      expect(fail).toThrow("non-weekly budgets not supported");
    });
    it("errors if the periods are out-of-order", () => {
      const p1 = {budget: 100, startTimeMs: 50};
      const p2 = {budget: 50, startTimeMs: 25};
      const entry = {prefix: NodeAddress.empty, periods: [p1, p2]};
      const budget = {intervalLength: "WEEKLY", entries: [entry]};
      const fail = () => applyBudget(WG.empty(), budget);
      expect(fail).toThrow("periods out-of-order");
    });
    describe("end-to-end testing with weighted graphs", () => {
      class TestWeightedGraph {
        wg: WeightedGraphT;
        constructor() {
          this.wg = {weights: emptyWeights(), graph: new Graph()};
        }
        addNode(opts: {|
          +id: number,
          +timestampMs: TimestampMs,
          +mint: number,
        |}): TestWeightedGraph {
          const {id, timestampMs, mint} = opts;
          const address = this.addressForId(id);
          this.wg.weights.nodeWeights.set(address, mint);
          this.wg.graph.addNode({
            address,
            description: String(id),
            timestampMs,
          });
          return this;
        }
        addressForId(id: number): NodeAddressT {
          return NodeAddress.fromParts([String(id)]);
        }
      }
      // Since we are hardcoded to week-based time partitioning, generate some
      // week-spaced timestamps
      const w1 = +utcWeek.floor(0);
      const w2 = +utcWeek.ceil(0);
      const w3 = +utcWeek.ceil(w2 + 1);
      const w4 = +utcWeek.ceil(w3 + 1);
      expect(w4).toBeGreaterThan(w3);
      expect(w3).toBeGreaterThan(w2);
      expect(w2).toBeGreaterThan(w1);

      it("works in a sample case where weights must be reduced", () => {
        const twg = new TestWeightedGraph()
          .addNode({id: 1, timestampMs: w1, mint: 100})
          .addNode({id: 2, timestampMs: w2, mint: 200});
        const period = {startTimeMs: -Infinity, budget: 50};
        const entry = {prefix: NodeAddress.empty, periods: [period]};
        const budget = {intervalLength: "WEEKLY", entries: [entry]};
        const reweightedGraph = applyBudget(twg.wg, budget);
        const reweightEvaluator = nodeWeightEvaluator(reweightedGraph.weights);
        const a1 = twg.addressForId(1);
        expect(reweightEvaluator(a1)).toEqual(50); // w1 conforms to budget
        const a2 = twg.addressForId(2);
        expect(reweightEvaluator(a2)).toEqual(50); // w2 conforms to budget
      });
      it("works in a case where multiple weights are in contention", () => {
        const twg = new TestWeightedGraph()
          .addNode({id: 1, timestampMs: w1, mint: 10})
          .addNode({id: 2, timestampMs: w1, mint: 90});
        const period = {startTimeMs: -Infinity, budget: 10};
        const entry = {prefix: NodeAddress.empty, periods: [period]};
        const budget = {intervalLength: "WEEKLY", entries: [entry]};
        const reweightedGraph = applyBudget(twg.wg, budget);
        const reweightEvaluator = nodeWeightEvaluator(reweightedGraph.weights);
        const a1 = twg.addressForId(1);
        expect(reweightEvaluator(a1)).toEqual(1);
        const a2 = twg.addressForId(2);
        expect(reweightEvaluator(a2)).toEqual(9);
      });
      it("does not modify periods that are under budget", () => {
        const twg = new TestWeightedGraph()
          .addNode({id: 1, timestampMs: w1, mint: 5})
          .addNode({id: 2, timestampMs: w2, mint: 15});
        const period = {startTimeMs: -Infinity, budget: 10};
        const entry = {prefix: NodeAddress.empty, periods: [period]};
        const budget = {intervalLength: "WEEKLY", entries: [entry]};
        const reweightedGraph = applyBudget(twg.wg, budget);
        const reweightEvaluator = nodeWeightEvaluator(reweightedGraph.weights);
        const a1 = twg.addressForId(1);
        expect(reweightEvaluator(a1)).toEqual(5);
        const a2 = twg.addressForId(2);
        expect(reweightEvaluator(a2)).toEqual(10);
      });
    });
  });

  describe("_anyCommonPrefixes", () => {
    it("returns true if there are common prefixes", () => {
      // Empty address is prefix of everything
      const empty = NodeAddress.empty;
      const foo = NodeAddress.fromParts(["foo"]);
      // Order of arguments must not matter.
      expect(_anyCommonPrefixes([empty, foo])).toBe(true);
      expect(_anyCommonPrefixes([foo, empty])).toBe(true);
    });
    it("returns true if the same prefix is present multiple times", () => {
      const empty = NodeAddress.empty;
      // Order of arguments must not matter.
      expect(_anyCommonPrefixes([empty, empty])).toBe(true);
    });
    it("returns false for no common prefixes", () => {
      expect(
        _anyCommonPrefixes([
          NodeAddress.fromParts(["bar"]),
          NodeAddress.fromParts(["foo"]),
        ])
      ).toBe(false);
    });
    it("returns false in the empty case", () => {
      expect(_anyCommonPrefixes([])).toBe(false);
    });
  });

  describe("_findCurrentBudget", () => {
    it("returns infinity if there are no periods", () => {
      expect(_findCurrentBudget([], 0)).toEqual(Infinity);
    });
    it("returns infinity if no periods are yet active", () => {
      expect(_findCurrentBudget([{startTimeMs: 1, budget: 100}], 0)).toEqual(
        Infinity
      );
    });
    it("returns the latest period whose startTimeMs is <= current time", () => {
      const p1 = {startTimeMs: 0, budget: 1};
      const p2 = {startTimeMs: 1, budget: 2};
      const p3 = {startTimeMs: 1, budget: 3};
      const p4 = {startTimeMs: 2, budget: 4};
      expect(_findCurrentBudget([p1, p2, p3, p4], 1)).toEqual(3);
    });
    it("returns the last period if necessary", () => {
      const p1 = {startTimeMs: 0, budget: 1};
      const p2 = {startTimeMs: 1, budget: 2};
      const p3 = {startTimeMs: 1, budget: 3};
      const p4 = {startTimeMs: 2, budget: 4};
      expect(_findCurrentBudget([p1, p2, p3, p4], 9)).toEqual(4);
    });
  });
});
