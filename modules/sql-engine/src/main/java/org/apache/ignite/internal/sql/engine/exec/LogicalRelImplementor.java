/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.combinedRowType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;
import static org.apache.ignite.internal.util.ArrayUtils.asList;
import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.ExpressionFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlComparator;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionRegistry;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractSetOpNode;
import org.apache.ignite.internal.sql.engine.exec.rel.CorrelatedNestedLoopJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.DataSourceScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.FilterNode;
import org.apache.ignite.internal.sql.engine.exec.rel.HashAggregateNode;
import org.apache.ignite.internal.sql.engine.exec.rel.HashJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox;
import org.apache.ignite.internal.sql.engine.exec.rel.IndexScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.IndexSpoolNode;
import org.apache.ignite.internal.sql.engine.exec.rel.IntersectNode;
import org.apache.ignite.internal.sql.engine.exec.rel.LimitNode;
import org.apache.ignite.internal.sql.engine.exec.rel.MergeJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.MinusNode;
import org.apache.ignite.internal.sql.engine.exec.rel.ModifyNode;
import org.apache.ignite.internal.sql.engine.exec.rel.NestedLoopJoinNode;
import org.apache.ignite.internal.sql.engine.exec.rel.Node;
import org.apache.ignite.internal.sql.engine.exec.rel.Outbox;
import org.apache.ignite.internal.sql.engine.exec.rel.ProjectNode;
import org.apache.ignite.internal.sql.engine.exec.rel.ScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.SortAggregateNode;
import org.apache.ignite.internal.sql.engine.exec.rel.SortNode;
import org.apache.ignite.internal.sql.engine.exec.rel.TableScanNode;
import org.apache.ignite.internal.sql.engine.exec.rel.TableSpoolNode;
import org.apache.ignite.internal.sql.engine.exec.rel.UnionAllNode;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.prepare.bounds.SearchBounds;
import org.apache.ignite.internal.sql.engine.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteFilter;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteHashJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueGet;
import org.apache.ignite.internal.sql.engine.rel.IgniteKeyValueModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteLimit;
import org.apache.ignite.internal.sql.engine.rel.IgniteMergeJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.sql.engine.rel.IgniteProject;
import org.apache.ignite.internal.sql.engine.rel.IgniteReceiver;
import org.apache.ignite.internal.sql.engine.rel.IgniteRel;
import org.apache.ignite.internal.sql.engine.rel.IgniteRelVisitor;
import org.apache.ignite.internal.sql.engine.rel.IgniteSelectCount;
import org.apache.ignite.internal.sql.engine.rel.IgniteSender;
import org.apache.ignite.internal.sql.engine.rel.IgniteSort;
import org.apache.ignite.internal.sql.engine.rel.IgniteSortedIndexSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteSystemViewScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableFunctionScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableModify;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableSpool;
import org.apache.ignite.internal.sql.engine.rel.IgniteTrimExchange;
import org.apache.ignite.internal.sql.engine.rel.IgniteUnionAll;
import org.apache.ignite.internal.sql.engine.rel.IgniteValues;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteColocatedSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteMapSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceHashAggregate;
import org.apache.ignite.internal.sql.engine.rel.agg.IgniteReduceSortAggregate;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteMapSetOp;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteReduceIntersect;
import org.apache.ignite.internal.sql.engine.rel.set.IgniteSetOp;
import org.apache.ignite.internal.sql.engine.rule.LogicalScanConverterRule;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteDataSource;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.TraitUtils;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;

/**
 * Implements a query plan.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class LogicalRelImplementor<RowT> implements IgniteRelVisitor<Node<RowT>> {
    public static final String CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG =
            "only INNER and LEFT join supported by IgniteCorrelatedNestedLoop";

    private final ExecutionContext<RowT> ctx;

    private final DestinationFactory<RowT> destinationFactory;

    private final ExchangeService exchangeSvc;

    private final MailboxRegistry mailboxRegistry;

    private final ExpressionFactory<RowT> expressionFactory;

    private final ResolvedDependencies resolvedDependencies;

    private final TableFunctionRegistry tableFunctionRegistry;

    /**
     * Constructor.
     *
     * @param ctx Root context.
     * @param mailboxRegistry Mailbox registry.
     * @param exchangeSvc Exchange service.
     * @param resolvedDependencies Dependencies required to execute this query.
     * @param tableFunctionRegistry Table function registry.
     */
    public LogicalRelImplementor(
            ExecutionContext<RowT> ctx,
            MailboxRegistry mailboxRegistry,
            ExchangeService exchangeSvc,
            ResolvedDependencies resolvedDependencies,
            TableFunctionRegistry tableFunctionRegistry
    ) {
        this.mailboxRegistry = mailboxRegistry;
        this.exchangeSvc = exchangeSvc;
        this.ctx = ctx;
        this.resolvedDependencies = resolvedDependencies;
        this.tableFunctionRegistry = tableFunctionRegistry;

        expressionFactory = ctx.expressionFactory();
        destinationFactory = new DestinationFactory<>(ctx.rowHandler(), resolvedDependencies);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSender rel) {
        IgniteDistribution distribution = rel.distribution();

        ColocationGroup targetGroup = ctx.target();

        assert targetGroup != null;

        Destination<RowT> dest = destinationFactory.createDestination(distribution, targetGroup);

        // Outbox fragment ID is used as exchange ID as well.
        Outbox<RowT> outbox = new Outbox<>(ctx, exchangeSvc, mailboxRegistry, rel.exchangeId(), rel.targetFragmentId(), dest);

        Node<RowT> input = visit(rel.getInput());

        outbox.register(input);

        mailboxRegistry.register(outbox);

        return outbox;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteFilter rel) {
        Predicate<RowT> pred = row -> expressionFactory.predicate(rel.getCondition(), rel.getRowType()).test(ctx, row);

        FilterNode<RowT> node = new FilterNode<>(ctx, pred);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTrimExchange rel) {
        assert TraitUtils.distribution(rel).getType() == HASH_DISTRIBUTED;

        ColocationGroup targetGroup = ctx.group(rel.sourceId());

        assert targetGroup != null;

        Destination<RowT> dest = destinationFactory.createDestination(rel.distribution(), targetGroup);

        String localNodeName = ctx.localNode().name();

        FilterNode<RowT> node = new FilterNode<>(ctx, r -> Objects.equals(localNodeName, first(dest.targets(r))));

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteProject rel) {
        Function<RowT, RowT> prj = row -> expressionFactory.project(rel.getProjects(), rel.getInput().getRowType()).project(ctx, row);

        ProjectNode<RowT> node = new ProjectNode<>(ctx, prj);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteNestedLoopJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        BiPredicate<RowT, RowT> cond = (left, right) ->
                expressionFactory.joinPredicate(rel.getCondition(), rowType).test(ctx, left, right);

        Node<RowT> node = NestedLoopJoinNode.create(ctx, outType, leftType, rightType, joinType, cond);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteHashJoin rel) {
        RelDataType outType = rel.getRowType();
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        RexNode nonEquiConditionExpression = RexUtil.composeConjunction(
                Commons.rexBuilder(), rel.analyzeCondition().nonEquiConditions, true
        );

        BiPredicate<RowT, RowT> nonEquiCondition = null;
        if (nonEquiConditionExpression != null) {
            RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);

            nonEquiCondition = (left, right) ->
                    expressionFactory.joinPredicate(rel.getCondition(), rowType).test(ctx, left, right);
        }

        Node<RowT> node = HashJoinNode.create(ctx, outType, leftType, rightType, joinType, rel.analyzeCondition(), nonEquiCondition);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteCorrelatedNestedLoopJoin rel) {
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightType));

        RelDataType rowType = combinedRowType(ctx.getTypeFactory(), leftType, rightType);
        BiPredicate<RowT, RowT> cond = (left, right) ->
                expressionFactory.joinPredicate(rel.getCondition(), rowType).test(ctx, left, right);

        assert rel.getJoinType() == JoinRelType.INNER || rel.getJoinType() == JoinRelType.LEFT
                : CNLJ_NOT_SUPPORTED_JOIN_ASSERTION_MSG;

        RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);
        RowFactory<RowT> outputRowFactory = ctx.rowHandler().factory(rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType)));

        Node<RowT> node = new CorrelatedNestedLoopJoinNode<>(ctx, cond, rel.getVariablesSet(),
                rel.getJoinType(), rightRowFactory, outputRowFactory);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMergeJoin rel) {
        RelDataType leftType = rel.getLeft().getRowType();
        RelDataType rightType = rel.getRight().getRowType();
        JoinRelType joinType = rel.getJoinType();

        int pairsCnt = rel.analyzeCondition().pairs().size();

        ImmutableBitSet leftKeys = rel.analyzeCondition().leftSet();

        List<RexNode> conjunctions = RelOptUtil.conjunctions(rel.getCondition());

        ImmutableBitSet.Builder nullCompAsEqualBuilder = ImmutableBitSet.builder();

        ImmutableBitSet nullCompAsEqual;
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int idx = ref.getIndex();
                if (leftKeys.get(idx)) {
                    nullCompAsEqualBuilder.set(idx);
                }
                return ref;
            }
        };

        for (RexNode expr : conjunctions) {
            if (expr.getKind() == SqlKind.IS_NOT_DISTINCT_FROM) {
                shuttle.apply(expr);
            }
        }

        nullCompAsEqual = nullCompAsEqualBuilder.build();

        Comparator<RowT> comp = (r1, r2) -> expressionFactory.comparator(
                rel.leftCollation().getFieldCollations().subList(0, pairsCnt),
                rel.rightCollation().getFieldCollations().subList(0, pairsCnt),
                nullCompAsEqual
        ).compare(ctx, r1, r2);

        Node<RowT> node = MergeJoinNode.create(ctx, leftType, rightType, joinType, comp);

        Node<RowT> leftInput = visit(rel.getLeft());
        Node<RowT> rightInput = visit(rel.getRight());

        node.register(asList(leftInput, rightInput));

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteIndexScan rel) {
        IgniteTable tbl = rel.getTable().unwrap(IgniteTable.class);

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();
        ImmutableBitSet requiredColumns = rel.requiredColumns();
        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);
        ScannableTable scannableTable = resolvedDependencies.scannableTable(tbl.id());

        IgniteIndex idx = tbl.indexes().get(rel.indexName());

        List<SearchBounds> searchBounds = rel.searchBounds();
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();

        Predicate<RowT> filters = condition == null ? null : row -> expressionFactory.predicate(condition, rowType).test(ctx, row);
        Function<RowT, RowT> prj = projects == null ? null : row -> expressionFactory.project(projects, rowType).project(ctx, row);

        RangeIterable<RowT> ranges = null;

        if (searchBounds != null) {
            SqlComparator<RowT> searchRowComparator = idx.type() == Type.SORTED
                    ? expressionFactory.comparator(IgniteIndex.createSearchRowCollation(idx.collation()))
                    : null;

            ranges = expressionFactory.ranges(searchBounds, idx.rowType(typeFactory, tbl.descriptor()), searchRowComparator).get(ctx);
        }

        RelCollation outputCollation = rel.collation();

        if (projects != null || requiredColumns != null) {
            outputCollation = outputCollation.apply(LogicalScanConverterRule.createMapping(
                    projects,
                    requiredColumns,
                    tbl.getRowType(typeFactory).getFieldCount()
            ));
        }

        ColocationGroup group = ctx.group(rel.sourceId());

        assert group != null;


        Comparator<RowT> comp = null;
        if (idx.type() == Type.SORTED && outputCollation != null && !nullOrEmpty(outputCollation.getFieldCollations())) {
            SqlComparator<RowT> searchRowComparator = expressionFactory.comparator(outputCollation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2); 

        }

        if (!group.nodeNames().contains(ctx.localNode().name())) {
            return new ScanNode<>(ctx, Collections.emptyList());
        }

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);
        PartitionProvider<RowT> partitionProvider = ctx.getPartitionProvider(rel.sourceId(), group, tbl);

        return new IndexScanNode<>(
                ctx,
                rowFactory,
                idx,
                scannableTable,
                tbl.descriptor(),
                partitionProvider,
                comp,
                ranges,
                filters,
                prj,
                requiredColumns == null ? null : requiredColumns.toBitSet()
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableBitSet requiredColumns = rel.requiredColumns();

        IgniteTable tbl = rel.getTable().unwrapOrThrow(IgniteTable.class);
        ScannableTable scannableTable = resolvedDependencies.scannableTable(tbl.id());

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = tbl.getRowType(typeFactory, requiredColumns);

        Predicate<RowT> filters = condition == null ? null : row -> expressionFactory.predicate(condition, rowType).test(ctx, row);
        Function<RowT, RowT> prj = projects == null ? null : row -> expressionFactory.project(projects, rowType).project(ctx, row);

        long sourceId = rel.sourceId();
        ColocationGroup group = ctx.group(sourceId);

        assert group != null;

        if (!group.nodeNames().contains(ctx.localNode().name())) {
            return new ScanNode<>(ctx, Collections.emptyList());
        }

        // TODO: IGNITE-22822 fix required columns.
        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        PartitionProvider<RowT> partitionProvider = ctx.getPartitionProvider(rel.sourceId(), group, tbl);

        return new TableScanNode<>(
                ctx,
                rowFactory,
                scannableTable,
                partitionProvider,
                filters,
                prj,
                requiredColumns == null ? null : requiredColumns.toBitSet()
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSystemViewScan rel) {
        RexNode condition = rel.condition();
        List<RexNode> projects = rel.projects();
        ImmutableBitSet requiredColumns = rel.requiredColumns();
        IgniteDataSource igniteDataSource = rel.getTable().unwrapOrThrow(IgniteDataSource.class);

        BinaryTupleSchema schema = fromTableDescriptor(igniteDataSource.descriptor());

        ScannableDataSource dataSource = resolvedDependencies.dataSource(igniteDataSource.id());

        IgniteTypeFactory typeFactory = ctx.getTypeFactory();

        RelDataType rowType = igniteDataSource.getRowType(typeFactory, requiredColumns);

        Predicate<RowT> filters = condition == null ? null : row -> expressionFactory.predicate(condition, rowType).test(ctx, row);
        Function<RowT, RowT> prj = projects == null ? null : row -> expressionFactory.project(projects, rowType).project(ctx, row);

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);
        return new DataSourceScanNode<>(
                ctx,
                rowFactory,
                schema,
                dataSource,
                filters,
                prj,
                requiredColumns == null ? null : requiredColumns.toBitSet()
        );
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteValues rel) {
        List<List<RexLiteral>> vals = Commons.cast(rel.getTuples());

        RelDataType rowType = rel.getRowType();

        return new ScanNode<>(ctx, expressionFactory.values(vals, rowType).get(ctx));
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteUnionAll rel) {
        UnionAllNode<RowT> node = new UnionAllNode<>(ctx);

        List<Node<RowT>> inputs = Commons.transform(rel.getInputs(), this::visit);

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteLimit rel) {
        Supplier<Integer> offset = (rel.offset() == null) ? null : () -> expressionFactory.<Integer>scalar(rel.offset()).get(ctx);
        Supplier<Integer> fetch = (rel.fetch() == null) ? null : () -> expressionFactory.<Integer>scalar(rel.fetch()).get(ctx);

        LimitNode<RowT> node = new LimitNode<>(ctx, offset, fetch);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSort rel) {
        RelCollation collation = rel.getCollation();

        Supplier<Integer> offset = (rel.offset == null) ? null : () -> expressionFactory.<Integer>scalar(rel.offset).get(ctx);
        Supplier<Integer> fetch = (rel.fetch == null) ? null : () -> expressionFactory.<Integer>scalar(rel.fetch).get(ctx);

        SortNode<RowT> node = new SortNode<>(
                ctx,
                (r1, r2) -> expressionFactory.comparator(collation).compare(ctx, r1, r2),
                offset,
                fetch
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableSpool rel) {
        TableSpoolNode<RowT> node = new TableSpoolNode<>(ctx, rel.readType == Spool.Type.LAZY);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSortedIndexSpool rel) {
        RelCollation collation = rel.collation();

        assert rel.searchBounds() != null : rel;

        Predicate<RowT> filter = row -> expressionFactory.predicate(rel.condition(), rel.getRowType()).test(ctx, row);
        SqlComparator<RowT> comparator = expressionFactory.comparator(collation);
        RangeIterable<RowT> ranges = expressionFactory.ranges(rel.searchBounds(), rel.getRowType(), comparator).get(ctx);

        IndexSpoolNode<RowT> node = IndexSpoolNode.createTreeSpool(
                ctx,
                rel.getRowType(),
                collation,
                (r1, r2) -> comparator.compare(ctx, r1, r2),
                filter,
                ranges
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteHashIndexSpool rel) {
        Supplier<RowT> searchRow = () -> expressionFactory.rowSource(rel.searchRow()).get(ctx);

        Predicate<RowT> filter = row -> expressionFactory.predicate(rel.condition(), rel.getRowType()).test(ctx, row);

        IndexSpoolNode<RowT> node = IndexSpoolNode.createHashSpool(
                ctx,
                ImmutableBitSet.of(rel.keys()),
                filter,
                searchRow,
                rel.allowNulls()
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteSetOp rel) {
        RelDataType rowType = rel.getRowType();

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        List<Node<RowT>> inputs = Commons.transform(rel.getInputs(), this::visit);
        int columnNum;

        if (rel instanceof IgniteMapSetOp) {
            columnNum = rel.getInput(0).getRowType().getFieldCount();
        } else {
            columnNum = rowType.getFieldCount();
        }

        AbstractSetOpNode<RowT> node;

        if (rel instanceof Minus) {
            node = new MinusNode<>(ctx, columnNum, rel.aggregateType(), rel.all(), rowFactory);
        } else if (rel instanceof IgniteIntersect) {
            int inputsNum;

            if (rel instanceof IgniteReduceIntersect) {
                // MAP phase of intersect operator produces (c1, c2, .., cN, counters_input1, ... counters_inputM),
                // so the number of input relations is equal to the number of input cols to reduce phase
                // minus the number of output columns produced by a set operator (See IgniteMapSetOp::buildRowType).
                int inputCols = rel.getInput(0).getRowType().getFieldCount();
                int outputCols = rel.getRowType().getFieldCount();

                inputsNum = inputCols - outputCols;
            } else {
                inputsNum = rel.getInputs().size();
            }

            node = new IntersectNode<>(ctx, columnNum, rel.aggregateType(), rel.all(), rowFactory, inputsNum);
        } else {
            throw new AssertionError("Unexpected set node: " + rel);
        }

        node.register(inputs);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableFunctionScan rel) {
        TableFunction<RowT> tableFunction = tableFunctionRegistry.getTableFunction(ctx, (RexCall) rel.getCall());

        return new ScanNode<>(ctx, tableFunction);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteTableModify rel) {
        IgniteTable table = rel.getTable().unwrapOrThrow(IgniteTable.class);
        UpdatableTable updatableTable = resolvedDependencies.updatableTable(table.id());
        RelDataType rowType = rel.getInput().getRowType();
        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        ModifyNode<RowT> node = new ModifyNode<>(
                ctx, updatableTable, rel.sourceId(), rel.getOperation(), rel.getUpdateColumnList(), rowFactory
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReceiver rel) {
        RelDataType rowType = rel.getRowType();

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));

        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator<RowT> searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        Inbox<RowT> inbox = new Inbox<>(ctx, exchangeSvc, mailboxRegistry,
                ctx.remotes(rel.exchangeId()), comp,
                rowFactory, rel.exchangeId(), rel.sourceFragmentId());

        mailboxRegistry.register(inbox);

        return inbox;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteColocatedHashAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accumulators, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMapHashAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accumulators, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReduceHashAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggregateCalls().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.accumulatorsFactory(
                    type, rel.getAggregateCalls(), rel.getInput().getRowType()
            ).get(ctx);
        }

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        HashAggregateNode<RowT> node = new HashAggregateNode<>(ctx, type, rel.getGroupSets(), accumulators, rowFactory);

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteColocatedSortAggregate rel) {
        AggregateType type = AggregateType.SINGLE;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator<RowT> searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accumulators,
                rowFactory,
                comp
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteMapSortAggregate rel) {
        AggregateType type = AggregateType.MAP;

        RelDataType rowType = rel.getRowType();
        RelDataType inputType = rel.getInput().getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggCallList().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.accumulatorsFactory(
                    type, rel.getAggCallList(), inputType
            ).get(ctx);
        }

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator<RowT> searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accumulators,
                rowFactory,
                comp
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteReduceSortAggregate rel) {
        AggregateType type = AggregateType.REDUCE;

        RelDataType rowType = rel.getRowType();

        List<AccumulatorWrapper<RowT>> accumulators;
        if (rel.getAggregateCalls().isEmpty()) {
            accumulators = List.of();
        } else {
            accumulators = expressionFactory.accumulatorsFactory(
                    type, rel.getAggregateCalls(), rel.getInput().getRowType()
            ).get(ctx);
        }

        RowSchema rowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rowType));
        RowFactory<RowT> rowFactory = ctx.rowHandler().factory(rowSchema);

        RelCollation collation = rel.collation();
        Comparator<RowT> comp = null;
        if (collation != null && !nullOrEmpty(collation.getFieldCollations())) {
            SqlComparator<RowT> searchRowComparator = expressionFactory.comparator(collation);

            comp = (r1, r2) -> searchRowComparator.compare(ctx, r1, r2);
        }

        if (rel.getGroupSet().isEmpty() && comp == null) {
            comp = (k1, k2) -> 0;
        }

        SortAggregateNode<RowT> node = new SortAggregateNode<>(
                ctx,
                type,
                rel.getGroupSet(),
                accumulators,
                rowFactory,
                comp
        );

        Node<RowT> input = visit(rel.getInput());

        node.register(input);

        return node;
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteExchange rel) {
        throw new AssertionError(rel.getClass());
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteKeyValueGet rel) {
        throw new AssertionError(rel.getClass());
    }

    /** {@inheritDoc} */
    @Override
    public Node<RowT> visit(IgniteKeyValueModify rel) {
        throw new AssertionError(rel.getClass());
    }

    @Override
    public Node<RowT> visit(IgniteSelectCount rel) {
        throw new AssertionError(rel.getClass());
    }

    private Node<RowT> visit(RelNode rel) {
        return visit((IgniteRel) rel);
    }

    @SuppressWarnings("unchecked")
    public <T extends Node<RowT>> T go(IgniteRel rel) {
        return (T) visit(rel);
    }

    private static BinaryTupleSchema fromTableDescriptor(TableDescriptor descriptor) {
        Element[] elements = new Element[descriptor.columnsCount()];

        int idx = 0;
        for (ColumnDescriptor column : descriptor) {
            elements[idx++] = new Element(column.physicalType(), column.nullable());
        }

        return BinaryTupleSchema.create(elements);
    }
}
