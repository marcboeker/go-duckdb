#ifdef GODUCKDB_FROM_SOURCE
// See https://raw.githubusercontent.com/duckdb/duckdb/main/LICENSE for licensing information

#include "duckdb.hpp"
#include "duckdb-internal.hpp"
#ifndef DUCKDB_AMALGAMATION
#error header mismatch
#endif












namespace duckdb {

static void GatherAliases(BoundQueryNode &node, case_insensitive_map_t<idx_t> &aliases,
                          parsed_expression_map_t<idx_t> &expressions, const vector<idx_t> &reorder_idx) {
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// setop, recurse
		auto &setop = node.Cast<BoundSetOperationNode>();

		// create new reorder index
		if (setop.setop_type == SetOperationType::UNION_BY_NAME) {
			vector<idx_t> new_left_reorder_idx(setop.left_reorder_idx.size());
			vector<idx_t> new_right_reorder_idx(setop.right_reorder_idx.size());
			for (idx_t i = 0; i < setop.left_reorder_idx.size(); ++i) {
				new_left_reorder_idx[i] = reorder_idx[setop.left_reorder_idx[i]];
			}

			for (idx_t i = 0; i < setop.right_reorder_idx.size(); ++i) {
				new_right_reorder_idx[i] = reorder_idx[setop.right_reorder_idx[i]];
			}

			// use new reorder index
			GatherAliases(*setop.left, aliases, expressions, new_left_reorder_idx);
			GatherAliases(*setop.right, aliases, expressions, new_right_reorder_idx);
			return;
		}

		GatherAliases(*setop.left, aliases, expressions, reorder_idx);
		GatherAliases(*setop.right, aliases, expressions, reorder_idx);
	} else {
		// query node
		D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
		auto &select = node.Cast<BoundSelectNode>();
		// fill the alias lists
		for (idx_t i = 0; i < select.names.size(); i++) {
			auto &name = select.names[i];
			auto &expr = select.original_expressions[i];
			// first check if the alias is already in there
			auto entry = aliases.find(name);

			idx_t index = reorder_idx[i];

			if (entry != aliases.end()) {
				// the alias already exists
				// check if there is a conflict

				if (entry->second != index) {
					// there is a conflict
					// we place "-1" in the aliases map at this location
					// "-1" signifies that there is an ambiguous reference
					aliases[name] = DConstants::INVALID_INDEX;
				}
			} else {
				// the alias is not in there yet, just assign it
				aliases[name] = index;
			}
			// now check if the node is already in the set of expressions
			auto expr_entry = expressions.find(*expr);
			if (expr_entry != expressions.end()) {
				// the node is in there
				// repeat the same as with the alias: if there is an ambiguity we insert "-1"
				if (expr_entry->second != index) {
					expressions[*expr] = DConstants::INVALID_INDEX;
				}
			} else {
				// not in there yet, just place it in there
				expressions[*expr] = index;
			}
		}
	}
}

static void BuildUnionByNameInfo(BoundSetOperationNode &result, bool can_contain_nulls) {
	D_ASSERT(result.setop_type == SetOperationType::UNION_BY_NAME);
	case_insensitive_map_t<idx_t> left_names_map;
	case_insensitive_map_t<idx_t> right_names_map;

	BoundQueryNode *left_node = result.left.get();
	BoundQueryNode *right_node = result.right.get();

	// Build a name_map to use to check if a name exists
	// We throw a binder exception if two same name in the SELECT list
	for (idx_t i = 0; i < left_node->names.size(); ++i) {
		if (left_names_map.find(left_node->names[i]) != left_names_map.end()) {
			throw BinderException("UNION(ALL) BY NAME operation doesn't support same name in SELECT list");
		}
		left_names_map[left_node->names[i]] = i;
	}

	for (idx_t i = 0; i < right_node->names.size(); ++i) {
		if (right_names_map.find(right_node->names[i]) != right_names_map.end()) {
			throw BinderException("UNION(ALL) BY NAME operation doesn't support same name in SELECT list");
		}
		if (left_names_map.find(right_node->names[i]) == left_names_map.end()) {
			result.names.push_back(right_node->names[i]);
		}
		right_names_map[right_node->names[i]] = i;
	}

	idx_t new_size = result.names.size();
	bool need_reorder = false;
	vector<idx_t> left_reorder_idx(left_node->names.size());
	vector<idx_t> right_reorder_idx(right_node->names.size());

	// Construct return type and reorder_idxs
	// reorder_idxs is used to gather correct alias_map
	// and expression_map in GatherAlias(...)
	for (idx_t i = 0; i < new_size; ++i) {
		auto left_index = left_names_map.find(result.names[i]);
		auto right_index = right_names_map.find(result.names[i]);
		bool left_exist = left_index != left_names_map.end();
		bool right_exist = right_index != right_names_map.end();
		LogicalType result_type;
		if (left_exist && right_exist) {
			result_type = LogicalType::MaxLogicalType(left_node->types[left_index->second],
			                                          right_node->types[right_index->second]);
			if (left_index->second != i || right_index->second != i) {
				need_reorder = true;
			}
			left_reorder_idx[left_index->second] = i;
			right_reorder_idx[right_index->second] = i;
		} else if (left_exist) {
			result_type = left_node->types[left_index->second];
			need_reorder = true;
			left_reorder_idx[left_index->second] = i;
		} else {
			D_ASSERT(right_exist);
			result_type = right_node->types[right_index->second];
			need_reorder = true;
			right_reorder_idx[right_index->second] = i;
		}

		if (!can_contain_nulls) {
			if (ExpressionBinder::ContainsNullType(result_type)) {
				result_type = ExpressionBinder::ExchangeNullType(result_type);
			}
		}

		result.types.push_back(result_type);
	}

	result.left_reorder_idx = std::move(left_reorder_idx);
	result.right_reorder_idx = std::move(right_reorder_idx);

	// If reorder is required, collect reorder expressions for push projection
	// into the two child nodes of union node
	if (need_reorder) {
		for (idx_t i = 0; i < new_size; ++i) {
			auto left_index = left_names_map.find(result.names[i]);
			auto right_index = right_names_map.find(result.names[i]);
			bool left_exist = left_index != left_names_map.end();
			bool right_exist = right_index != right_names_map.end();
			unique_ptr<Expression> left_reorder_expr;
			unique_ptr<Expression> right_reorder_expr;
			if (left_exist && right_exist) {
				left_reorder_expr = make_uniq<BoundColumnRefExpression>(
				    left_node->types[left_index->second], ColumnBinding(left_node->GetRootIndex(), left_index->second));
				right_reorder_expr =
				    make_uniq<BoundColumnRefExpression>(right_node->types[right_index->second],
				                                        ColumnBinding(right_node->GetRootIndex(), right_index->second));
			} else if (left_exist) {
				left_reorder_expr = make_uniq<BoundColumnRefExpression>(
				    left_node->types[left_index->second], ColumnBinding(left_node->GetRootIndex(), left_index->second));
				// create null value here
				right_reorder_expr = make_uniq<BoundConstantExpression>(Value(result.types[i]));
			} else {
				D_ASSERT(right_exist);
				left_reorder_expr = make_uniq<BoundConstantExpression>(Value(result.types[i]));
				right_reorder_expr =
				    make_uniq<BoundColumnRefExpression>(right_node->types[right_index->second],
				                                        ColumnBinding(right_node->GetRootIndex(), right_index->second));
			}
			result.left_reorder_exprs.push_back(std::move(left_reorder_expr));
			result.right_reorder_exprs.push_back(std::move(right_reorder_expr));
		}
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SetOperationNode &statement) {
	auto result = make_uniq<BoundSetOperationNode>();
	result->setop_type = statement.setop_type;

	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);

	result->setop_index = GenerateTableIndex();

	result->left_binder = Binder::CreateBinder(context, this);
	result->left_binder->can_contain_nulls = true;
	result->left = result->left_binder->BindNode(*statement.left);
	result->right_binder = Binder::CreateBinder(context, this);
	result->right_binder->can_contain_nulls = true;
	result->right = result->right_binder->BindNode(*statement.right);

	result->names = result->left->names;

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	MoveCorrelatedExpressions(*result->right_binder);

	// now both sides have been bound we can resolve types
	if (result->setop_type != SetOperationType::UNION_BY_NAME &&
	    result->left->types.size() != result->right->types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (result->setop_type == SetOperationType::UNION_BY_NAME) {
		BuildUnionByNameInfo(*result, can_contain_nulls);

	} else {
		// figure out the types of the setop result by picking the max of both
		for (idx_t i = 0; i < result->left->types.size(); i++) {
			auto result_type = LogicalType::MaxLogicalType(result->left->types[i], result->right->types[i]);
			if (!can_contain_nulls) {
				if (ExpressionBinder::ContainsNullType(result_type)) {
					result_type = ExpressionBinder::ExchangeNullType(result_type);
				}
			}
			result->types.push_back(result_type);
		}
	}

	if (!statement.modifiers.empty()) {
		// handle the ORDER BY/DISTINCT clauses

		// we recursively visit the children of this node to extract aliases and expressions that can be referenced
		// in the ORDER BY
		case_insensitive_map_t<idx_t> alias_map;
		parsed_expression_map_t<idx_t> expression_map;

		if (result->setop_type == SetOperationType::UNION_BY_NAME) {
			GatherAliases(*result->left, alias_map, expression_map, result->left_reorder_idx);
			GatherAliases(*result->right, alias_map, expression_map, result->right_reorder_idx);
		} else {
			vector<idx_t> reorder_idx;
			for (idx_t i = 0; i < result->names.size(); i++) {
				reorder_idx.push_back(i);
			}
			GatherAliases(*result, alias_map, expression_map, reorder_idx);
		}
		// now we perform the actual resolution of the ORDER BY/DISTINCT expressions
		OrderBinder order_binder({result->left_binder.get(), result->right_binder.get()}, result->setop_index,
		                         alias_map, expression_map, result->names.size());
		BindModifiers(order_binder, statement, *result);
	}

	// finally bind the types of the ORDER/DISTINCT clause expressions
	BindModifierTypes(*result, result->types, result->setop_index);
	return std::move(result);
}

} // namespace duckdb
















namespace duckdb {

unique_ptr<QueryNode> Binder::BindTableMacro(FunctionExpression &function, TableMacroCatalogEntry &macro_func,
                                             idx_t depth) {

	auto &macro_def = macro_func.function->Cast<TableMacroFunction>();
	auto node = macro_def.query_node->Copy();

	// auto &macro_def = *macro_func->function;

	// validate the arguments and separate positional and default arguments
	vector<unique_ptr<ParsedExpression>> positionals;
	unordered_map<string, unique_ptr<ParsedExpression>> defaults;
	string error =
	    MacroFunction::ValidateArguments(*macro_func.function, macro_func.name, function, positionals, defaults);
	if (!error.empty()) {
		// cannot use error below as binder rnot in scope
		// return BindResult(binder. FormatError(*expr->get(), error));
		throw BinderException(FormatError(function, error));
	}

	// create a MacroBinding to bind this macro's parameters to its arguments
	vector<LogicalType> types;
	vector<string> names;
	// positional parameters
	for (idx_t i = 0; i < macro_def.parameters.size(); i++) {
		types.emplace_back(LogicalType::SQLNULL);
		auto &param = macro_def.parameters[i]->Cast<ColumnRefExpression>();
		names.push_back(param.GetColumnName());
	}
	// default parameters
	for (auto it = macro_def.default_parameters.begin(); it != macro_def.default_parameters.end(); it++) {
		types.emplace_back(LogicalType::SQLNULL);
		names.push_back(it->first);
		// now push the defaults into the positionals
		positionals.push_back(std::move(defaults[it->first]));
	}
	auto new_macro_binding = make_uniq<DummyBinding>(types, names, macro_func.name);
	new_macro_binding->arguments = &positionals;

	// We need an ExpressionBinder so that we can call ExpressionBinder::ReplaceMacroParametersRecursive()
	auto eb = ExpressionBinder(*this, this->context);

	eb.macro_binding = new_macro_binding.get();

	/* Does it all goes throu every expression in a selectstmt  */
	ParsedExpressionIterator::EnumerateQueryNodeChildren(
	    *node, [&](unique_ptr<ParsedExpression> &child) { eb.ReplaceMacroParametersRecursive(child); });

	return node;
}

} // namespace duckdb








namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTENode &node) {
	// Generate the logical plan for the cte_query and child.
	auto cte_query = CreatePlan(*node.query);
	auto cte_child = CreatePlan(*node.child);

	auto root = make_uniq<LogicalMaterializedCTE>(node.ctename, node.setop_index, node.types.size(),
	                                              std::move(cte_query), std::move(cte_child));

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins =
	    node.child_binder->has_unplanned_dependent_joins || node.query_binder->has_unplanned_dependent_joins;

	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb








namespace duckdb {

unique_ptr<LogicalOperator> Binder::VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root) {
	D_ASSERT(root);
	for (auto &mod : node.modifiers) {
		switch (mod->type) {
		case ResultModifierType::DISTINCT_MODIFIER: {
			auto &bound = mod->Cast<BoundDistinctModifier>();
			auto distinct = make_uniq<LogicalDistinct>(std::move(bound.target_distincts), bound.distinct_type);
			distinct->AddChild(std::move(root));
			root = std::move(distinct);
			break;
		}
		case ResultModifierType::ORDER_MODIFIER: {
			auto &bound = mod->Cast<BoundOrderModifier>();
			if (root->type == LogicalOperatorType::LOGICAL_DISTINCT) {
				auto &distinct = root->Cast<LogicalDistinct>();
				if (distinct.distinct_type == DistinctType::DISTINCT_ON) {
					auto order_by = make_uniq<BoundOrderModifier>();
					for (auto &order_node : bound.orders) {
						order_by->orders.push_back(order_node.Copy());
					}
					distinct.order_by = std::move(order_by);
				}
			}
			auto order = make_uniq<LogicalOrder>(std::move(bound.orders));
			order->AddChild(std::move(root));
			root = std::move(order);
			break;
		}
		case ResultModifierType::LIMIT_MODIFIER: {
			auto &bound = mod->Cast<BoundLimitModifier>();
			auto limit = make_uniq<LogicalLimit>(bound.limit_val, bound.offset_val, std::move(bound.limit),
			                                     std::move(bound.offset));
			limit->AddChild(std::move(root));
			root = std::move(limit);
			break;
		}
		case ResultModifierType::LIMIT_PERCENT_MODIFIER: {
			auto &bound = mod->Cast<BoundLimitPercentModifier>();
			auto limit = make_uniq<LogicalLimitPercent>(bound.limit_percent, bound.offset_val, std::move(bound.limit),
			                                            std::move(bound.offset));
			limit->AddChild(std::move(root));
			root = std::move(limit);
			break;
		}
		default:
			throw BinderException("Unimplemented modifier type!");
		}
	}
	return root;
}

} // namespace duckdb








namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundRecursiveCTENode &node) {
	// Generate the logical plan for the left and right sides of the set operation
	node.left_binder->is_outside_flattened = is_outside_flattened;
	node.right_binder->is_outside_flattened = is_outside_flattened;

	auto left_node = node.left_binder->CreatePlan(*node.left);
	auto right_node = node.right_binder->CreatePlan(*node.right);

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins =
	    node.left_binder->has_unplanned_dependent_joins || node.right_binder->has_unplanned_dependent_joins;

	// for both the left and right sides, cast them to the same types
	left_node = CastLogicalOperatorToTypes(node.left->types, node.types, std::move(left_node));
	right_node = CastLogicalOperatorToTypes(node.right->types, node.types, std::move(right_node));

	if (!node.right_binder->bind_context.cte_references[node.ctename] ||
	    *node.right_binder->bind_context.cte_references[node.ctename] == 0) {
		auto root = make_uniq<LogicalSetOperation>(node.setop_index, node.types.size(), std::move(left_node),
		                                           std::move(right_node), LogicalOperatorType::LOGICAL_UNION);
		return VisitQueryNode(node, std::move(root));
	}
	auto root = make_uniq<LogicalRecursiveCTE>(node.ctename, node.setop_index, node.types.size(), node.union_all,
	                                           std::move(left_node), std::move(right_node));

	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb








namespace duckdb {

unique_ptr<LogicalOperator> Binder::PlanFilter(unique_ptr<Expression> condition, unique_ptr<LogicalOperator> root) {
	PlanSubqueries(condition, root);
	auto filter = make_uniq<LogicalFilter>(std::move(condition));
	filter->AddChild(std::move(root));
	return std::move(filter);
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSelectNode &statement) {
	unique_ptr<LogicalOperator> root;
	D_ASSERT(statement.from_table);
	root = CreatePlan(*statement.from_table);
	D_ASSERT(root);

	// plan the sample clause
	if (statement.sample_options) {
		root = make_uniq<LogicalSample>(std::move(statement.sample_options), std::move(root));
	}

	if (statement.where_clause) {
		root = PlanFilter(std::move(statement.where_clause), std::move(root));
	}

	if (!statement.aggregates.empty() || !statement.groups.group_expressions.empty()) {
		if (!statement.groups.group_expressions.empty()) {
			// visit the groups
			for (auto &group : statement.groups.group_expressions) {
				PlanSubqueries(group, root);
			}
		}
		// now visit all aggregate expressions
		for (auto &expr : statement.aggregates) {
			PlanSubqueries(expr, root);
		}
		// finally create the aggregate node with the group_index and aggregate_index as obtained from the binder
		auto aggregate = make_uniq<LogicalAggregate>(statement.group_index, statement.aggregate_index,
		                                             std::move(statement.aggregates));
		aggregate->groups = std::move(statement.groups.group_expressions);
		aggregate->groupings_index = statement.groupings_index;
		aggregate->grouping_sets = std::move(statement.groups.grouping_sets);
		aggregate->grouping_functions = std::move(statement.grouping_functions);

		aggregate->AddChild(std::move(root));
		root = std::move(aggregate);
	} else if (!statement.groups.grouping_sets.empty()) {
		// edge case: we have grouping sets but no groups or aggregates
		// this can only happen if we have e.g. select 1 from tbl group by ();
		// just output a dummy scan
		root = make_uniq_base<LogicalOperator, LogicalDummyScan>(statement.group_index);
	}

	if (statement.having) {
		PlanSubqueries(statement.having, root);
		auto having = make_uniq<LogicalFilter>(std::move(statement.having));

		having->AddChild(std::move(root));
		root = std::move(having);
	}

	if (!statement.windows.empty()) {
		auto win = make_uniq<LogicalWindow>(statement.window_index);
		win->expressions = std::move(statement.windows);
		// visit the window expressions
		for (auto &expr : win->expressions) {
			PlanSubqueries(expr, root);
		}
		D_ASSERT(!win->expressions.empty());
		win->AddChild(std::move(root));
		root = std::move(win);
	}

	if (statement.qualify) {
		PlanSubqueries(statement.qualify, root);
		auto qualify = make_uniq<LogicalFilter>(std::move(statement.qualify));

		qualify->AddChild(std::move(root));
		root = std::move(qualify);
	}

	for (idx_t i = statement.unnests.size(); i > 0; i--) {
		auto unnest_level = i - 1;
		auto entry = statement.unnests.find(unnest_level);
		if (entry == statement.unnests.end()) {
			throw InternalException("unnests specified at level %d but none were found", unnest_level);
		}
		auto &unnest_node = entry->second;
		auto unnest = make_uniq<LogicalUnnest>(unnest_node.index);
		unnest->expressions = std::move(unnest_node.expressions);
		// visit the unnest expressions
		for (auto &expr : unnest->expressions) {
			PlanSubqueries(expr, root);
		}
		D_ASSERT(!unnest->expressions.empty());
		unnest->AddChild(std::move(root));
		root = std::move(unnest);
	}

	for (auto &expr : statement.select_list) {
		PlanSubqueries(expr, root);
	}

	auto proj = make_uniq<LogicalProjection>(statement.projection_index, std::move(statement.select_list));
	auto &projection = *proj;
	proj->AddChild(std::move(root));
	root = std::move(proj);

	// finish the plan by handling the elements of the QueryNode
	root = VisitQueryNode(statement, std::move(root));

	// add a prune node if necessary
	if (statement.need_prune) {
		D_ASSERT(root);
		vector<unique_ptr<Expression>> prune_expressions;
		for (idx_t i = 0; i < statement.column_count; i++) {
			prune_expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    projection.expressions[i]->return_type, ColumnBinding(statement.projection_index, i)));
		}
		auto prune = make_uniq<LogicalProjection>(statement.prune_index, std::move(prune_expressions));
		prune->AddChild(std::move(root));
		root = std::move(prune);
	}
	return root;
}

} // namespace duckdb







namespace duckdb {

// Optionally push a PROJECTION operator
unique_ptr<LogicalOperator> Binder::CastLogicalOperatorToTypes(vector<LogicalType> &source_types,
                                                               vector<LogicalType> &target_types,
                                                               unique_ptr<LogicalOperator> op) {
	D_ASSERT(op);
	// first check if we even need to cast
	D_ASSERT(source_types.size() == target_types.size());
	if (source_types == target_types) {
		// source and target types are equal: don't need to cast
		return op;
	}
	// otherwise add casts
	auto node = op.get();
	if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		// "node" is a projection; we can just do the casts in there
		D_ASSERT(node->expressions.size() == source_types.size());
		// add the casts to the selection list
		for (idx_t i = 0; i < target_types.size(); i++) {
			if (source_types[i] != target_types[i]) {
				// differing types, have to add a cast
				string alias = node->expressions[i]->alias;
				node->expressions[i] =
				    BoundCastExpression::AddCastToType(context, std::move(node->expressions[i]), target_types[i]);
				node->expressions[i]->alias = alias;
			}
		}
		return op;
	} else {
		// found a non-projection operator
		// push a new projection containing the casts

		// fetch the set of column bindings
		auto setop_columns = op->GetColumnBindings();
		D_ASSERT(setop_columns.size() == source_types.size());

		// now generate the expression list
		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < target_types.size(); i++) {
			unique_ptr<Expression> result = make_uniq<BoundColumnRefExpression>(source_types[i], setop_columns[i]);
			if (source_types[i] != target_types[i]) {
				// add a cast only if the source and target types are not equivalent
				result = BoundCastExpression::AddCastToType(context, std::move(result), target_types[i]);
			}
			select_list.push_back(std::move(result));
		}
		auto projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(select_list));
		projection->children.push_back(std::move(op));
		return std::move(projection);
	}
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSetOperationNode &node) {
	// Generate the logical plan for the left and right sides of the set operation
	node.left_binder->is_outside_flattened = is_outside_flattened;
	node.right_binder->is_outside_flattened = is_outside_flattened;

	auto left_node = node.left_binder->CreatePlan(*node.left);
	auto right_node = node.right_binder->CreatePlan(*node.right);

	// Add a new projection to child node
	D_ASSERT(node.left_reorder_exprs.size() == node.right_reorder_exprs.size());
	if (!node.left_reorder_exprs.empty()) {
		D_ASSERT(node.setop_type == SetOperationType::UNION_BY_NAME);
		vector<LogicalType> left_types;
		vector<LogicalType> right_types;
		// We are going to add a new projection operator, so collect the type
		// of reorder exprs in order to call CastLogicalOperatorToTypes()
		for (idx_t i = 0; i < node.left_reorder_exprs.size(); ++i) {
			left_types.push_back(node.left_reorder_exprs[i]->return_type);
			right_types.push_back(node.right_reorder_exprs[i]->return_type);
		}

		auto left_projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(node.left_reorder_exprs));
		left_projection->children.push_back(std::move(left_node));
		left_node = std::move(left_projection);

		auto right_projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(node.right_reorder_exprs));
		right_projection->children.push_back(std::move(right_node));
		right_node = std::move(right_projection);

		left_node = CastLogicalOperatorToTypes(left_types, node.types, std::move(left_node));
		right_node = CastLogicalOperatorToTypes(right_types, node.types, std::move(right_node));
	} else {
		left_node = CastLogicalOperatorToTypes(node.left->types, node.types, std::move(left_node));
		right_node = CastLogicalOperatorToTypes(node.right->types, node.types, std::move(right_node));
	}

	// check if there are any unplanned subqueries left in either child
	has_unplanned_dependent_joins =
	    node.left_binder->has_unplanned_dependent_joins || node.right_binder->has_unplanned_dependent_joins;

	// create actual logical ops for setops
	LogicalOperatorType logical_type;
	switch (node.setop_type) {
	case SetOperationType::UNION:
	case SetOperationType::UNION_BY_NAME:
		logical_type = LogicalOperatorType::LOGICAL_UNION;
		break;
	case SetOperationType::EXCEPT:
		logical_type = LogicalOperatorType::LOGICAL_EXCEPT;
		break;
	default:
		D_ASSERT(node.setop_type == SetOperationType::INTERSECT);
		logical_type = LogicalOperatorType::LOGICAL_INTERSECT;
		break;
	}

	auto root = make_uniq<LogicalSetOperation>(node.setop_index, node.types.size(), std::move(left_node),
	                                           std::move(right_node), logical_type);

	return VisitQueryNode(node, std::move(root));
}

} // namespace duckdb





















namespace duckdb {

static unique_ptr<Expression> PlanUncorrelatedSubquery(Binder &binder, BoundSubqueryExpression &expr,
                                                       unique_ptr<LogicalOperator> &root,
                                                       unique_ptr<LogicalOperator> plan) {
	D_ASSERT(!expr.IsCorrelated());
	switch (expr.subquery_type) {
	case SubqueryType::EXISTS: {
		// uncorrelated EXISTS
		// we only care about existence, hence we push a LIMIT 1 operator
		auto limit = make_uniq<LogicalLimit>(1, 0, nullptr, nullptr);
		limit->AddChild(std::move(plan));
		plan = std::move(limit);

		// now we push a COUNT(*) aggregate onto the limit, this will be either 0 or 1 (EXISTS or NOT EXISTS)
		auto count_star_fun = CountStarFun::GetFunction();

		FunctionBinder function_binder(binder.context);
		auto count_star =
		    function_binder.BindAggregateFunction(count_star_fun, {}, nullptr, AggregateType::NON_DISTINCT);
		auto idx_type = count_star->return_type;
		vector<unique_ptr<Expression>> aggregate_list;
		aggregate_list.push_back(std::move(count_star));
		auto aggregate_index = binder.GenerateTableIndex();
		auto aggregate =
		    make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), aggregate_index, std::move(aggregate_list));
		aggregate->AddChild(std::move(plan));
		plan = std::move(aggregate);

		// now we push a projection with a comparison to 1
		auto left_child = make_uniq<BoundColumnRefExpression>(idx_type, ColumnBinding(aggregate_index, 0));
		auto right_child = make_uniq<BoundConstantExpression>(Value::Numeric(idx_type, 1));
		auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(left_child),
		                                                       std::move(right_child));

		vector<unique_ptr<Expression>> projection_list;
		projection_list.push_back(std::move(comparison));
		auto projection_index = binder.GenerateTableIndex();
		auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_list));
		projection->AddChild(std::move(plan));
		plan = std::move(projection);

		// we add it to the main query by adding a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant
		root = LogicalCrossProduct::Create(std::move(root), std::move(plan));

		// we replace the original subquery with a ColumnRefExpression referring to the result of the projection (either
		// TRUE or FALSE)
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), LogicalType::BOOLEAN,
		                                           ColumnBinding(projection_index, 0));
	}
	case SubqueryType::SCALAR: {
		// uncorrelated scalar, we want to return the first entry
		// figure out the table index of the bound table of the entry which we want to return
		auto bindings = plan->GetColumnBindings();
		D_ASSERT(bindings.size() == 1);
		idx_t table_idx = bindings[0].table_index;

		// in the uncorrelated case we are only interested in the first result of the query
		// hence we simply push a LIMIT 1 to get the first row of the subquery
		auto limit = make_uniq<LogicalLimit>(1, 0, nullptr, nullptr);
		limit->AddChild(std::move(plan));
		plan = std::move(limit);

		// we push an aggregate that returns the FIRST element
		vector<unique_ptr<Expression>> expressions;
		auto bound = make_uniq<BoundColumnRefExpression>(expr.return_type, ColumnBinding(table_idx, 0));
		vector<unique_ptr<Expression>> first_children;
		first_children.push_back(std::move(bound));

		FunctionBinder function_binder(binder.context);
		auto first_agg = function_binder.BindAggregateFunction(
		    FirstFun::GetFunction(expr.return_type), std::move(first_children), nullptr, AggregateType::NON_DISTINCT);

		expressions.push_back(std::move(first_agg));
		auto aggr_index = binder.GenerateTableIndex();
		auto aggr = make_uniq<LogicalAggregate>(binder.GenerateTableIndex(), aggr_index, std::move(expressions));
		aggr->AddChild(std::move(plan));
		plan = std::move(aggr);

		// in the uncorrelated case, we add the value to the main query through a cross product
		// FIXME: should use something else besides cross product as we always add only one scalar constant and cross
		// product is not optimized for this.
		D_ASSERT(root);
		root = LogicalCrossProduct::Create(std::move(root), std::move(plan));

		// we replace the original subquery with a BoundColumnRefExpression referring to the first result of the
		// aggregation
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(aggr_index, 0));
	}
	default: {
		D_ASSERT(expr.subquery_type == SubqueryType::ANY);
		// we generate a MARK join that results in either (TRUE, FALSE or NULL)
		// subquery has NULL values -> result is (TRUE or NULL)
		// subquery has no NULL values -> result is (TRUE, FALSE or NULL [if input is NULL])
		// fetch the column bindings
		auto plan_columns = plan->GetColumnBindings();

		// then we generate the MARK join with the subquery
		idx_t mark_index = binder.GenerateTableIndex();
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::MARK);
		join->mark_index = mark_index;
		join->AddChild(std::move(root));
		join->AddChild(std::move(plan));
		// create the JOIN condition
		JoinCondition cond;
		cond.left = std::move(expr.child);
		cond.right = BoundCastExpression::AddDefaultCastToType(
		    make_uniq<BoundColumnRefExpression>(expr.child_type, plan_columns[0]), expr.child_target);
		cond.comparison = expr.comparison_type;
		join->conditions.push_back(std::move(cond));
		root = std::move(join);

		// we replace the original subquery with a BoundColumnRefExpression referring to the mark column
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(mark_index, 0));
	}
	}
}

static unique_ptr<LogicalComparisonJoin>
CreateDuplicateEliminatedJoin(const vector<CorrelatedColumnInfo> &correlated_columns, JoinType join_type,
                              unique_ptr<LogicalOperator> original_plan, bool perform_delim) {
	auto delim_join = make_uniq<LogicalComparisonJoin>(join_type, LogicalOperatorType::LOGICAL_DELIM_JOIN);
	if (!perform_delim) {
		// if we are not performing a delim join, we push a row_number() OVER() window operator on the LHS
		// and perform all duplicate elimination on that row number instead
		D_ASSERT(correlated_columns[0].type.id() == LogicalTypeId::BIGINT);
		auto window = make_uniq<LogicalWindow>(correlated_columns[0].binding.table_index);
		auto row_number =
		    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
		row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
		row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
		row_number->alias = "delim_index";
		window->expressions.push_back(std::move(row_number));
		window->AddChild(std::move(original_plan));
		original_plan = std::move(window);
	}
	delim_join->AddChild(std::move(original_plan));
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		delim_join->duplicate_eliminated_columns.push_back(make_uniq<BoundColumnRefExpression>(col.type, col.binding));
		delim_join->mark_types.push_back(col.type);
	}
	return delim_join;
}

static void CreateDelimJoinConditions(LogicalComparisonJoin &delim_join,
                                      const vector<CorrelatedColumnInfo> &correlated_columns,
                                      vector<ColumnBinding> bindings, idx_t base_offset, bool perform_delim) {
	auto col_count = perform_delim ? correlated_columns.size() : 1;
	for (idx_t i = 0; i < col_count; i++) {
		auto &col = correlated_columns[i];
		auto binding_idx = base_offset + i;
		if (binding_idx >= bindings.size()) {
			throw InternalException("Delim join - binding index out of range");
		}
		JoinCondition cond;
		cond.left = make_uniq<BoundColumnRefExpression>(col.name, col.type, col.binding);
		cond.right = make_uniq<BoundColumnRefExpression>(col.name, col.type, bindings[binding_idx]);
		cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
		delim_join.conditions.push_back(std::move(cond));
	}
}

static bool PerformDelimOnType(const LogicalType &type) {
	if (type.InternalType() == PhysicalType::LIST) {
		return false;
	}
	if (type.InternalType() == PhysicalType::STRUCT) {
		for (auto &entry : StructType::GetChildTypes(type)) {
			if (!PerformDelimOnType(entry.second)) {
				return false;
			}
		}
	}
	return true;
}

static bool PerformDuplicateElimination(Binder &binder, vector<CorrelatedColumnInfo> &correlated_columns) {
	if (!ClientConfig::GetConfig(binder.context).enable_optimizer) {
		// if optimizations are disabled we always do a delim join
		return true;
	}
	bool perform_delim = true;
	for (auto &col : correlated_columns) {
		if (!PerformDelimOnType(col.type)) {
			perform_delim = false;
			break;
		}
	}
	if (perform_delim) {
		return true;
	}
	auto binding = ColumnBinding(binder.GenerateTableIndex(), 0);
	auto type = LogicalType::BIGINT;
	auto name = "delim_index";
	CorrelatedColumnInfo info(binding, type, name, 0);
	correlated_columns.insert(correlated_columns.begin(), std::move(info));
	return false;
}

static unique_ptr<Expression> PlanCorrelatedSubquery(Binder &binder, BoundSubqueryExpression &expr,
                                                     unique_ptr<LogicalOperator> &root,
                                                     unique_ptr<LogicalOperator> plan) {
	auto &correlated_columns = expr.binder->correlated_columns;
	// FIXME: there should be a way of disabling decorrelation for ANY queries as well, but not for now...
	bool perform_delim =
	    expr.subquery_type == SubqueryType::ANY ? true : PerformDuplicateElimination(binder, correlated_columns);
	D_ASSERT(expr.IsCorrelated());
	// correlated subquery
	// for a more in-depth explanation of this code, read the paper "Unnesting Arbitrary Subqueries"
	// we handle three types of correlated subqueries: Scalar, EXISTS and ANY
	// all three cases are very similar with some minor changes (mainly the type of join performed at the end)
	switch (expr.subquery_type) {
	case SubqueryType::SCALAR: {
		// correlated SCALAR query
		// first push a DUPLICATE ELIMINATED join
		// a duplicate eliminated join creates a duplicate eliminated copy of the LHS
		// and pushes it into any DUPLICATE_ELIMINATED SCAN operators on the RHS

		// in the SCALAR case, we create a SINGLE join (because we are only interested in obtaining the value)
		// NULL values are equal in this join because we join on the correlated columns ONLY
		// and e.g. in the query: SELECT (SELECT 42 FROM integers WHERE i1.i IS NULL LIMIT 1) FROM integers i1;
		// the input value NULL will generate the value 42, and we need to join NULL on the LHS with NULL on the RHS
		// the left side is the original plan
		// this is the side that will be duplicate eliminated and pushed into the RHS
		auto delim_join =
		    CreateDuplicateEliminatedJoin(correlated_columns, JoinType::SINGLE, std::move(root), perform_delim);

		// the right side initially is a DEPENDENT join between the duplicate eliminated scan and the subquery
		// HOWEVER: we do not explicitly create the dependent join
		// instead, we eliminate the dependent join by pushing it down into the right side of the plan
		FlattenDependentJoins flatten(binder, correlated_columns, perform_delim);

		// first we check which logical operators have correlated expressions in the first place
		flatten.DetectCorrelatedExpressions(plan.get());
		// now we push the dependent join down
		auto dependent_join = flatten.PushDownDependentJoin(std::move(plan));

		// now the dependent join is fully eliminated
		// we only need to create the join conditions between the LHS and the RHS
		// fetch the set of columns
		auto plan_columns = dependent_join->GetColumnBindings();

		// now create the join conditions
		CreateDelimJoinConditions(*delim_join, correlated_columns, plan_columns, flatten.delim_offset, perform_delim);
		delim_join->AddChild(std::move(dependent_join));
		root = std::move(delim_join);
		// finally push the BoundColumnRefExpression referring to the data element returned by the join
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, plan_columns[flatten.data_offset]);
	}
	case SubqueryType::EXISTS: {
		// correlated EXISTS query
		// this query is similar to the correlated SCALAR query, except we use a MARK join here
		idx_t mark_index = binder.GenerateTableIndex();
		auto delim_join =
		    CreateDuplicateEliminatedJoin(correlated_columns, JoinType::MARK, std::move(root), perform_delim);
		delim_join->mark_index = mark_index;
		// RHS
		FlattenDependentJoins flatten(binder, correlated_columns, perform_delim, true);
		flatten.DetectCorrelatedExpressions(plan.get());
		auto dependent_join = flatten.PushDownDependentJoin(std::move(plan));

		// fetch the set of columns
		auto plan_columns = dependent_join->GetColumnBindings();

		// now we create the join conditions between the dependent join and the original table
		CreateDelimJoinConditions(*delim_join, correlated_columns, plan_columns, flatten.delim_offset, perform_delim);
		delim_join->AddChild(std::move(dependent_join));
		root = std::move(delim_join);
		// finally push the BoundColumnRefExpression referring to the marker
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(mark_index, 0));
	}
	default: {
		D_ASSERT(expr.subquery_type == SubqueryType::ANY);
		// correlated ANY query
		// this query is similar to the correlated SCALAR query
		// however, in this case we push a correlated MARK join
		// note that in this join null values are NOT equal for ALL columns, but ONLY for the correlated columns
		// the correlated mark join handles this case by itself
		// as the MARK join has one extra join condition (the original condition, of the ANY expression, e.g.
		// [i=ANY(...)])
		idx_t mark_index = binder.GenerateTableIndex();
		auto delim_join =
		    CreateDuplicateEliminatedJoin(correlated_columns, JoinType::MARK, std::move(root), perform_delim);
		delim_join->mark_index = mark_index;
		// RHS
		FlattenDependentJoins flatten(binder, correlated_columns, true, true);
		flatten.DetectCorrelatedExpressions(plan.get());
		auto dependent_join = flatten.PushDownDependentJoin(std::move(plan));

		// fetch the columns
		auto plan_columns = dependent_join->GetColumnBindings();

		// now we create the join conditions between the dependent join and the original table
		CreateDelimJoinConditions(*delim_join, correlated_columns, plan_columns, flatten.delim_offset, perform_delim);
		// add the actual condition based on the ANY/ALL predicate
		JoinCondition compare_cond;
		compare_cond.left = std::move(expr.child);
		compare_cond.right = BoundCastExpression::AddDefaultCastToType(
		    make_uniq<BoundColumnRefExpression>(expr.child_type, plan_columns[0]), expr.child_target);
		compare_cond.comparison = expr.comparison_type;
		delim_join->conditions.push_back(std::move(compare_cond));

		delim_join->AddChild(std::move(dependent_join));
		root = std::move(delim_join);
		// finally push the BoundColumnRefExpression referring to the marker
		return make_uniq<BoundColumnRefExpression>(expr.GetName(), expr.return_type, ColumnBinding(mark_index, 0));
	}
	}
}

void RecursiveDependentJoinPlanner::VisitOperator(LogicalOperator &op) {
	if (!op.children.empty()) {
		root = std::move(op.children[0]);
		D_ASSERT(root);
		if (root->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			// Found a dependent join, flatten it
			auto &new_root = root->Cast<LogicalDependentJoin>();
			root = binder.PlanLateralJoin(std::move(new_root.children[0]), std::move(new_root.children[1]),
			                              new_root.correlated_columns, new_root.join_type,
			                              std::move(new_root.join_condition));
		}
		VisitOperatorExpressions(op);
		op.children[0] = std::move(root);
		for (idx_t i = 0; i < op.children.size(); i++) {
			D_ASSERT(op.children[i]);
			VisitOperator(*op.children[i]);
		}
	}
}

unique_ptr<Expression> RecursiveDependentJoinPlanner::VisitReplace(BoundSubqueryExpression &expr,
                                                                   unique_ptr<Expression> *expr_ptr) {
	return binder.PlanSubquery(expr, root);
}

unique_ptr<Expression> Binder::PlanSubquery(BoundSubqueryExpression &expr, unique_ptr<LogicalOperator> &root) {
	D_ASSERT(root);
	// first we translate the QueryNode of the subquery into a logical plan
	// note that we do not plan nested subqueries yet
	auto sub_binder = Binder::CreateBinder(context, this);
	sub_binder->is_outside_flattened = false;
	auto subquery_root = sub_binder->CreatePlan(*expr.subquery);
	D_ASSERT(subquery_root);

	// now we actually flatten the subquery
	auto plan = std::move(subquery_root);

	unique_ptr<Expression> result_expression;
	if (!expr.IsCorrelated()) {
		result_expression = PlanUncorrelatedSubquery(*this, expr, root, std::move(plan));
	} else {
		result_expression = PlanCorrelatedSubquery(*this, expr, root, std::move(plan));
	}
	// finally, we recursively plan the nested subqueries (if there are any)
	if (sub_binder->has_unplanned_dependent_joins) {
		RecursiveDependentJoinPlanner plan(*this);
		plan.VisitOperator(*root);
	}
	return result_expression;
}

void Binder::PlanSubqueries(unique_ptr<Expression> &expr_ptr, unique_ptr<LogicalOperator> &root) {
	if (!expr_ptr) {
		return;
	}
	auto &expr = *expr_ptr;
	// first visit the children of the node, if any
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &expr) { PlanSubqueries(expr, root); });

	// check if this is a subquery node
	if (expr.expression_class == ExpressionClass::BOUND_SUBQUERY) {
		auto &subquery = expr.Cast<BoundSubqueryExpression>();
		// subquery node! plan it
		if (subquery.IsCorrelated() && !is_outside_flattened) {
			// detected a nested correlated subquery
			// we don't plan it yet here, we are currently planning a subquery
			// nested subqueries will only be planned AFTER the current subquery has been flattened entirely
			has_unplanned_dependent_joins = true;
			return;
		}
		expr_ptr = PlanSubquery(subquery, root);
	}
}

unique_ptr<LogicalOperator> Binder::PlanLateralJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                                                    vector<CorrelatedColumnInfo> &correlated_columns,
                                                    JoinType join_type, unique_ptr<Expression> condition) {
	// scan the right operator for correlated columns
	// correlated LATERAL JOIN
	vector<JoinCondition> conditions;
	vector<unique_ptr<Expression>> arbitrary_expressions;
	if (condition) {
		// extract join conditions, if there are any
		LogicalComparisonJoin::ExtractJoinConditions(context, join_type, left, right, std::move(condition), conditions,
		                                             arbitrary_expressions);
	}

	auto perform_delim = PerformDuplicateElimination(*this, correlated_columns);
	auto delim_join = CreateDuplicateEliminatedJoin(correlated_columns, join_type, std::move(left), perform_delim);

	FlattenDependentJoins flatten(*this, correlated_columns, perform_delim);

	// first we check which logical operators have correlated expressions in the first place
	flatten.DetectCorrelatedExpressions(right.get(), true);
	// now we push the dependent join down
	auto dependent_join = flatten.PushDownDependentJoin(std::move(right));

	// now the dependent join is fully eliminated
	// we only need to create the join conditions between the LHS and the RHS
	// fetch the set of columns
	auto plan_columns = dependent_join->GetColumnBindings();

	// now create the join conditions
	// start off with the conditions that were passed in (if any)
	D_ASSERT(delim_join->conditions.empty());
	delim_join->conditions = std::move(conditions);
	// then add the delim join conditions
	CreateDelimJoinConditions(*delim_join, correlated_columns, plan_columns, flatten.delim_offset, perform_delim);
	delim_join->AddChild(std::move(dependent_join));

	// check if there are any arbitrary expressions left
	if (!arbitrary_expressions.empty()) {
		// we can only evaluate scalar arbitrary expressions for inner joins
		if (join_type != JoinType::INNER) {
			throw BinderException(
			    "Join condition for non-inner LATERAL JOIN must be a comparison between the left and right side");
		}
		auto filter = make_uniq<LogicalFilter>();
		filter->expressions = std::move(arbitrary_expressions);
		filter->AddChild(std::move(delim_join));
		return std::move(filter);
	}
	return std::move(delim_join);
}

} // namespace duckdb






namespace duckdb {

BoundStatement Binder::Bind(AttachStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ATTACH, std::move(stmt.info));
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb






namespace duckdb {

BoundStatement Binder::Bind(CallStatement &stmt) {
	BoundStatement result;

	TableFunctionRef ref;
	ref.function = std::move(stmt.function);

	auto bound_func = Bind(ref);
	auto &bound_table_func = bound_func->Cast<BoundTableFunction>();
	;
	auto &get = bound_table_func.get->Cast<LogicalGet>();
	D_ASSERT(get.returned_types.size() > 0);
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.column_ids.push_back(i);
	}

	result.types = get.returned_types;
	result.names = get.names;
	result.plan = CreatePlan(*bound_func);
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb






















#include <algorithm>

namespace duckdb {

vector<string> GetUniqueNames(const vector<string> &original_names) {
	unordered_set<string> name_set;
	vector<string> unique_names;
	unique_names.reserve(original_names.size());

	for (auto &name : original_names) {
		auto insert_result = name_set.insert(name);
		if (insert_result.second == false) {
			// Could not be inserted, name already exists
			idx_t index = 1;
			string postfixed_name;
			while (true) {
				postfixed_name = StringUtil::Format("%s:%d", name, index);
				auto res = name_set.insert(postfixed_name);
				if (!res.second) {
					index++;
					continue;
				}
				break;
			}
			unique_names.push_back(postfixed_name);
		} else {
			unique_names.push_back(name);
		}
	}
	return unique_names;
}

BoundStatement Binder::BindCopyTo(CopyStatement &stmt) {
	// COPY TO a file
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("COPY TO is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	// lookup the format in the catalog
	auto &copy_function =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, stmt.info->format);
	if (copy_function.function.plan) {
		// plan rewrite COPY TO
		return copy_function.function.plan(*this, stmt);
	}

	// bind the select statement
	auto select_node = Bind(*stmt.select_statement);

	if (!copy_function.function.copy_to_bind) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}
	bool use_tmp_file = true;
	bool overwrite_or_ignore = false;
	FilenamePattern filename_pattern;
	bool user_set_use_tmp_file = false;
	bool per_thread_output = false;
	vector<idx_t> partition_cols;

	auto original_options = stmt.info->options;
	stmt.info->options.clear();

	for (auto &option : original_options) {
		auto loption = StringUtil::Lower(option.first);
		if (loption == "use_tmp_file") {
			use_tmp_file =
			    option.second.empty() || option.second[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
			user_set_use_tmp_file = true;
			continue;
		}
		if (loption == "overwrite_or_ignore") {
			overwrite_or_ignore =
			    option.second.empty() || option.second[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
			continue;
		}
		if (loption == "filename_pattern") {
			if (option.second.empty()) {
				throw IOException("FILENAME_PATTERN cannot be empty");
			}
			filename_pattern.SetFilenamePattern(
			    option.second[0].CastAs(context, LogicalType::VARCHAR).GetValue<string>());
			continue;
		}

		if (loption == "per_thread_output") {
			per_thread_output =
			    option.second.empty() || option.second[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
			continue;
		}
		if (loption == "partition_by") {
			auto converted = ConvertVectorToValue(std::move(option.second));
			partition_cols = ParseColumnsOrdered(converted, select_node.names, loption);
			continue;
		}
		stmt.info->options[option.first] = option.second;
	}
	if (user_set_use_tmp_file && per_thread_output) {
		throw NotImplementedException("Can't combine USE_TMP_FILE and PER_THREAD_OUTPUT for COPY");
	}
	if (user_set_use_tmp_file && !partition_cols.empty()) {
		throw NotImplementedException("Can't combine USE_TMP_FILE and PARTITION_BY for COPY");
	}
	if (per_thread_output && !partition_cols.empty()) {
		throw NotImplementedException("Can't combine PER_THREAD_OUTPUT and PARTITION_BY for COPY");
	}
	bool is_remote_file = config.file_system->IsRemoteFile(stmt.info->file_path);
	if (is_remote_file) {
		use_tmp_file = false;
	} else {
		bool is_file_and_exists = config.file_system->FileExists(stmt.info->file_path);
		bool is_stdout = stmt.info->file_path == "/dev/stdout";
		if (!user_set_use_tmp_file) {
			use_tmp_file = is_file_and_exists && !per_thread_output && partition_cols.empty() && !is_stdout;
		}
	}

	auto unique_column_names = GetUniqueNames(select_node.names);

	auto function_data =
	    copy_function.function.copy_to_bind(context, *stmt.info, unique_column_names, select_node.types);
	// now create the copy information
	auto copy = make_uniq<LogicalCopyToFile>(copy_function.function, std::move(function_data));
	copy->file_path = stmt.info->file_path;
	copy->use_tmp_file = use_tmp_file;
	copy->overwrite_or_ignore = overwrite_or_ignore;
	copy->filename_pattern = filename_pattern;
	copy->per_thread_output = per_thread_output;
	copy->partition_output = !partition_cols.empty();
	copy->partition_columns = std::move(partition_cols);

	copy->names = unique_column_names;
	copy->expected_types = select_node.types;

	copy->AddChild(std::move(select_node.plan));

	result.plan = std::move(copy);

	return result;
}

BoundStatement Binder::BindCopyFrom(CopyStatement &stmt) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("COPY FROM is disabled by configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BIGINT};
	result.names = {"Count"};

	if (stmt.info->table.empty()) {
		throw ParserException("COPY FROM requires a table name to be specified");
	}
	// COPY FROM a file
	// generate an insert statement for the the to-be-inserted table
	InsertStatement insert;
	insert.table = stmt.info->table;
	insert.schema = stmt.info->schema;
	insert.catalog = stmt.info->catalog;
	insert.columns = stmt.info->select_list;

	// bind the insert statement to the base table
	auto insert_statement = Bind(insert);
	D_ASSERT(insert_statement.plan->type == LogicalOperatorType::LOGICAL_INSERT);

	auto &bound_insert = insert_statement.plan->Cast<LogicalInsert>();

	// lookup the format in the catalog
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &copy_function = catalog.GetEntry<CopyFunctionCatalogEntry>(context, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function.function.copy_from_bind) {
		throw NotImplementedException("COPY FROM is not supported for FORMAT \"%s\"", stmt.info->format);
	}
	// lookup the table to copy into
	BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
	auto &table =
	    Catalog::GetEntry<TableCatalogEntry>(context, stmt.info->catalog, stmt.info->schema, stmt.info->table);
	vector<string> expected_names;
	if (!bound_insert.column_index_map.empty()) {
		expected_names.resize(bound_insert.expected_types.size());
		for (auto &col : table.GetColumns().Physical()) {
			auto i = col.Physical();
			if (bound_insert.column_index_map[i] != DConstants::INVALID_INDEX) {
				expected_names[bound_insert.column_index_map[i]] = col.Name();
			}
		}
	} else {
		expected_names.reserve(bound_insert.expected_types.size());
		for (auto &col : table.GetColumns().Physical()) {
			expected_names.push_back(col.Name());
		}
	}

	auto function_data =
	    copy_function.function.copy_from_bind(context, *stmt.info, expected_names, bound_insert.expected_types);
	auto get = make_uniq<LogicalGet>(GenerateTableIndex(), copy_function.function.copy_from_function,
	                                 std::move(function_data), bound_insert.expected_types, expected_names);
	for (idx_t i = 0; i < bound_insert.expected_types.size(); i++) {
		get->column_ids.push_back(i);
	}
	insert_statement.plan->children.push_back(std::move(get));
	result.plan = std::move(insert_statement.plan);
	return result;
}

BoundStatement Binder::Bind(CopyStatement &stmt) {
	if (!stmt.info->is_from && !stmt.select_statement) {
		// copy table into file without a query
		// generate SELECT * FROM table;
		auto ref = make_uniq<BaseTableRef>();
		ref->catalog_name = stmt.info->catalog;
		ref->schema_name = stmt.info->schema;
		ref->table_name = stmt.info->table;

		auto statement = make_uniq<SelectNode>();
		statement->from_table = std::move(ref);
		if (!stmt.info->select_list.empty()) {
			for (auto &name : stmt.info->select_list) {
				statement->select_list.push_back(make_uniq<ColumnRefExpression>(name));
			}
		} else {
			statement->select_list.push_back(make_uniq<StarExpression>());
		}
		stmt.select_statement = std::move(statement);
	}
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	if (stmt.info->is_from) {
		return BindCopyFrom(stmt);
	} else {
		return BindCopyTo(stmt);
	}
}

} // namespace duckdb










































namespace duckdb {

void Binder::BindSchemaOrCatalog(ClientContext &context, string &catalog, string &schema) {
	if (catalog.empty() && !schema.empty()) {
		// schema is specified - but catalog is not
		// try searching for the catalog instead
		auto &db_manager = DatabaseManager::Get(context);
		auto database = db_manager.GetDatabase(context, schema);
		if (database) {
			// we have a database with this name
			// check if there is a schema
			auto schema_obj = Catalog::GetSchema(context, INVALID_CATALOG, schema, OnEntryNotFound::RETURN_NULL);
			if (schema_obj) {
				auto &attached = schema_obj->catalog.GetAttached();
				throw BinderException(
				    "Ambiguous reference to catalog or schema \"%s\" - use a fully qualified path like \"%s.%s\"",
				    schema, attached.GetName(), schema);
			}
			catalog = schema;
			schema = string();
		}
	}
}

void Binder::BindSchemaOrCatalog(string &catalog, string &schema) {
	BindSchemaOrCatalog(context, catalog, schema);
}

SchemaCatalogEntry &Binder::BindSchema(CreateInfo &info) {
	BindSchemaOrCatalog(info.catalog, info.schema);
	if (IsInvalidCatalog(info.catalog) && info.temporary) {
		info.catalog = TEMP_CATALOG;
	}
	auto &search_path = ClientData::Get(context).catalog_search_path;
	if (IsInvalidCatalog(info.catalog) && IsInvalidSchema(info.schema)) {
		auto &default_entry = search_path->GetDefault();
		info.catalog = default_entry.catalog;
		info.schema = default_entry.schema;
	} else if (IsInvalidSchema(info.schema)) {
		info.schema = search_path->GetDefaultSchema(info.catalog);
	} else if (IsInvalidCatalog(info.catalog)) {
		info.catalog = search_path->GetDefaultCatalog(info.schema);
	}
	if (IsInvalidCatalog(info.catalog)) {
		info.catalog = DatabaseManager::GetDefaultDatabase(context);
	}
	if (!info.temporary) {
		// non-temporary create: not read only
		if (info.catalog == TEMP_CATALOG) {
			throw ParserException("Only TEMPORARY table names can use the \"%s\" catalog", TEMP_CATALOG);
		}
	} else {
		if (info.catalog != TEMP_CATALOG) {
			throw ParserException("TEMPORARY table names can *only* use the \"%s\" catalog", TEMP_CATALOG);
		}
	}
	// fetch the schema in which we want to create the object
	auto &schema_obj = Catalog::GetSchema(context, info.catalog, info.schema);
	D_ASSERT(schema_obj.type == CatalogType::SCHEMA_ENTRY);
	info.schema = schema_obj.name;
	if (!info.temporary) {
		properties.modified_databases.insert(schema_obj.catalog.GetName());
	}
	return schema_obj;
}

SchemaCatalogEntry &Binder::BindCreateSchema(CreateInfo &info) {
	auto &schema = BindSchema(info);
	if (schema.catalog.IsSystemCatalog()) {
		throw BinderException("Cannot create entry in system catalog");
	}
	return schema;
}

void Binder::BindCreateViewInfo(CreateViewInfo &base) {
	// bind the view as if it were a query so we can catch errors
	// note that we bind the original, and replace the original with a copy
	auto view_binder = Binder::CreateBinder(context);
	view_binder->can_contain_nulls = true;

	auto copy = base.query->Copy();
	auto query_node = view_binder->Bind(*base.query);
	base.query = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(copy));
	if (base.aliases.size() > query_node.names.size()) {
		throw BinderException("More VIEW aliases than columns in query result");
	}
	// fill up the aliases with the remaining names of the bound query
	base.aliases.reserve(query_node.names.size());
	for (idx_t i = base.aliases.size(); i < query_node.names.size(); i++) {
		base.aliases.push_back(query_node.names[i]);
	}
	base.types = query_node.types;
}

SchemaCatalogEntry &Binder::BindCreateFunctionInfo(CreateInfo &info) {
	auto &base = info.Cast<CreateMacroInfo>();
	auto &scalar_function = base.function->Cast<ScalarMacroFunction>();

	if (scalar_function.expression->HasParameter()) {
		throw BinderException("Parameter expressions within macro's are not supported!");
	}

	// create macro binding in order to bind the function
	vector<LogicalType> dummy_types;
	vector<string> dummy_names;
	// positional parameters
	for (idx_t i = 0; i < base.function->parameters.size(); i++) {
		auto param = base.function->parameters[i]->Cast<ColumnRefExpression>();
		if (param.IsQualified()) {
			throw BinderException("Invalid parameter name '%s': must be unqualified", param.ToString());
		}
		dummy_types.emplace_back(LogicalType::SQLNULL);
		dummy_names.push_back(param.GetColumnName());
	}
	// default parameters
	for (auto it = base.function->default_parameters.begin(); it != base.function->default_parameters.end(); it++) {
		auto &val = it->second->Cast<ConstantExpression>();
		dummy_types.push_back(val.value.type());
		dummy_names.push_back(it->first);
	}
	auto this_macro_binding = make_uniq<DummyBinding>(dummy_types, dummy_names, base.name);
	macro_binding = this_macro_binding.get();
	ExpressionBinder::QualifyColumnNames(*this, scalar_function.expression);

	// create a copy of the expression because we do not want to alter the original
	auto expression = scalar_function.expression->Copy();

	// bind it to verify the function was defined correctly
	string error;
	auto sel_node = make_uniq<BoundSelectNode>();
	auto group_info = make_uniq<BoundGroupInformation>();
	SelectBinder binder(*this, context, *sel_node, *group_info);
	error = binder.Bind(expression, 0, false);

	if (!error.empty()) {
		throw BinderException(error);
	}

	return BindCreateSchema(info);
}

void Binder::BindLogicalType(ClientContext &context, LogicalType &type, optional_ptr<Catalog> catalog,
                             const string &schema) {
	if (type.id() == LogicalTypeId::LIST || type.id() == LogicalTypeId::MAP) {
		auto child_type = ListType::GetChildType(type);
		BindLogicalType(context, child_type, catalog, schema);
		auto alias = type.GetAlias();
		if (type.id() == LogicalTypeId::LIST) {
			type = LogicalType::LIST(child_type);
		} else {
			D_ASSERT(child_type.id() == LogicalTypeId::STRUCT); // map must be list of structs
			type = LogicalType::MAP(child_type);
		}

		type.SetAlias(alias);
	} else if (type.id() == LogicalTypeId::STRUCT) {
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			BindLogicalType(context, child_type.second, catalog, schema);
		}
		// Generate new Struct Type
		auto alias = type.GetAlias();
		type = LogicalType::STRUCT(child_types);
		type.SetAlias(alias);
	} else if (type.id() == LogicalTypeId::UNION) {
		auto member_types = UnionType::CopyMemberTypes(type);
		for (auto &member_type : member_types) {
			BindLogicalType(context, member_type.second, catalog, schema);
		}
		// Generate new Union Type
		auto alias = type.GetAlias();
		type = LogicalType::UNION(member_types);
		type.SetAlias(alias);
	} else if (type.id() == LogicalTypeId::USER) {
		auto user_type_name = UserType::GetTypeName(type);
		if (catalog) {
			// The search order is:
			// 1) In the same schema as the table
			// 2) In the same catalog
			// 3) System catalog
			type = catalog->GetType(context, schema, user_type_name, OnEntryNotFound::RETURN_NULL);

			if (type.id() == LogicalTypeId::INVALID) {
				type = catalog->GetType(context, INVALID_SCHEMA, user_type_name, OnEntryNotFound::RETURN_NULL);
			}

			if (type.id() == LogicalTypeId::INVALID) {
				type = Catalog::GetType(context, INVALID_CATALOG, schema, user_type_name);
			}
		} else {
			type = Catalog::GetType(context, INVALID_CATALOG, schema, user_type_name);
		}
		BindLogicalType(context, type, catalog, schema);
	}
}

static void FindMatchingPrimaryKeyColumns(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints,
                                          ForeignKeyConstraint &fk) {
	// find the matching primary key constraint
	bool found_constraint = false;
	// if no columns are defined, we will automatically try to bind to the primary key
	bool find_primary_key = fk.pk_columns.empty();
	for (auto &constr : constraints) {
		if (constr->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constr->Cast<UniqueConstraint>();
		if (find_primary_key && !unique.is_primary_key) {
			continue;
		}
		found_constraint = true;

		vector<string> pk_names;
		if (unique.index.index != DConstants::INVALID_INDEX) {
			pk_names.push_back(columns.GetColumn(LogicalIndex(unique.index)).Name());
		} else {
			pk_names = unique.columns;
		}
		if (find_primary_key) {
			// found matching primary key
			if (pk_names.size() != fk.fk_columns.size()) {
				auto pk_name_str = StringUtil::Join(pk_names, ",");
				auto fk_name_str = StringUtil::Join(fk.fk_columns, ",");
				throw BinderException(
				    "Failed to create foreign key: number of referencing (%s) and referenced columns (%s) differ",
				    fk_name_str, pk_name_str);
			}
			fk.pk_columns = pk_names;
			return;
		}
		if (pk_names.size() != fk.fk_columns.size()) {
			// the number of referencing and referenced columns for foreign keys must be the same
			continue;
		}
		bool equals = true;
		for (idx_t i = 0; i < fk.pk_columns.size(); i++) {
			if (!StringUtil::CIEquals(fk.pk_columns[i], pk_names[i])) {
				equals = false;
				break;
			}
		}
		if (!equals) {
			continue;
		}
		// found match
		return;
	}
	// no match found! examine why
	if (!found_constraint) {
		// no unique constraint or primary key
		string search_term = find_primary_key ? "primary key" : "primary key or unique constraint";
		throw BinderException("Failed to create foreign key: there is no %s for referenced table \"%s\"", search_term,
		                      fk.info.table);
	}
	// check if all the columns exist
	for (auto &name : fk.pk_columns) {
		bool found = columns.ColumnExists(name);
		if (!found) {
			throw BinderException(
			    "Failed to create foreign key: referenced table \"%s\" does not have a column named \"%s\"",
			    fk.info.table, name);
		}
	}
	auto fk_names = StringUtil::Join(fk.pk_columns, ",");
	throw BinderException("Failed to create foreign key: referenced table \"%s\" does not have a primary key or unique "
	                      "constraint on the columns %s",
	                      fk.info.table, fk_names);
}

static void FindForeignKeyIndexes(const ColumnList &columns, const vector<string> &names,
                                  vector<PhysicalIndex> &indexes) {
	D_ASSERT(indexes.empty());
	D_ASSERT(!names.empty());
	for (auto &name : names) {
		if (!columns.ColumnExists(name)) {
			throw BinderException("column \"%s\" named in key does not exist", name);
		}
		auto &column = columns.GetColumn(name);
		if (column.Generated()) {
			throw BinderException("Failed to create foreign key: referenced column \"%s\" is a generated column",
			                      column.Name());
		}
		indexes.push_back(column.Physical());
	}
}

static void CheckForeignKeyTypes(const ColumnList &pk_columns, const ColumnList &fk_columns, ForeignKeyConstraint &fk) {
	D_ASSERT(fk.info.pk_keys.size() == fk.info.fk_keys.size());
	for (idx_t c_idx = 0; c_idx < fk.info.pk_keys.size(); c_idx++) {
		auto &pk_col = pk_columns.GetColumn(fk.info.pk_keys[c_idx]);
		auto &fk_col = fk_columns.GetColumn(fk.info.fk_keys[c_idx]);
		if (pk_col.Type() != fk_col.Type()) {
			throw BinderException("Failed to create foreign key: incompatible types between column \"%s\" (\"%s\") and "
			                      "column \"%s\" (\"%s\")",
			                      pk_col.Name(), pk_col.Type().ToString(), fk_col.Name(), fk_col.Type().ToString());
		}
	}
}

void ExpressionContainsGeneratedColumn(const ParsedExpression &expr, const unordered_set<string> &gcols,
                                       bool &contains_gcol) {
	if (contains_gcol) {
		return;
	}
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &column_ref = expr.Cast<ColumnRefExpression>();
		auto &name = column_ref.GetColumnName();
		if (gcols.count(name)) {
			contains_gcol = true;
			return;
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { ExpressionContainsGeneratedColumn(child, gcols, contains_gcol); });
}

static bool AnyConstraintReferencesGeneratedColumn(CreateTableInfo &table_info) {
	unordered_set<string> generated_columns;
	for (auto &col : table_info.columns.Logical()) {
		if (!col.Generated()) {
			continue;
		}
		generated_columns.insert(col.Name());
	}
	if (generated_columns.empty()) {
		return false;
	}

	for (auto &constr : table_info.constraints) {
		switch (constr->type) {
		case ConstraintType::CHECK: {
			auto &constraint = constr->Cast<CheckConstraint>();
			auto &expr = constraint.expression;
			bool contains_generated_column = false;
			ExpressionContainsGeneratedColumn(*expr, generated_columns, contains_generated_column);
			if (contains_generated_column) {
				return true;
			}
			break;
		}
		case ConstraintType::NOT_NULL: {
			auto &constraint = constr->Cast<NotNullConstraint>();
			if (table_info.columns.GetColumn(constraint.index).Generated()) {
				return true;
			}
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &constraint = constr->Cast<UniqueConstraint>();
			auto index = constraint.index;
			if (index.index == DConstants::INVALID_INDEX) {
				for (auto &col : constraint.columns) {
					if (generated_columns.count(col)) {
						return true;
					}
				}
			} else {
				if (table_info.columns.GetColumn(index).Generated()) {
					return true;
				}
			}
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			// If it contained a generated column, an exception would have been thrown inside AddDataTableIndex earlier
			break;
		}
		default: {
			throw NotImplementedException("ConstraintType not implemented");
		}
		}
	}
	return false;
}

unique_ptr<LogicalOperator> DuckCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &base = stmt.info->Cast<CreateIndexInfo>();

	auto &get = plan->Cast<LogicalGet>();
	// bind the index expressions
	IndexBinder index_binder(binder, binder.context);
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(base.expressions.size());
	for (auto &expr : base.expressions) {
		expressions.push_back(index_binder.Bind(expr));
	}

	auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info));
	for (auto &column_id : get.column_ids) {
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			throw BinderException("Cannot create an index on the rowid!");
		}
		create_index_info->scan_types.push_back(get.returned_types[column_id]);
	}
	create_index_info->scan_types.emplace_back(LogicalType::ROW_TYPE);
	create_index_info->names = get.names;
	create_index_info->column_ids = get.column_ids;
	auto &bind_data = get.bind_data->Cast<TableScanBindData>();
	bind_data.is_create_index = true;
	get.column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);

	// the logical CREATE INDEX also needs all fields to scan the referenced table
	auto result = make_uniq<LogicalCreateIndex>(std::move(create_index_info), std::move(expressions), table);
	result->children.push_back(std::move(plan));
	return std::move(result);
}

BoundStatement Binder::Bind(CreateStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	auto catalog_type = stmt.info->type;
	switch (catalog_type) {
	case CatalogType::SCHEMA_ENTRY:
		result.plan = make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SCHEMA, std::move(stmt.info));
		break;
	case CatalogType::VIEW_ENTRY: {
		auto &base = stmt.info->Cast<CreateViewInfo>();
		// bind the schema
		auto &schema = BindCreateSchema(*stmt.info);
		BindCreateViewInfo(base);
		result.plan = make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_VIEW, std::move(stmt.info), &schema);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto &schema = BindCreateSchema(*stmt.info);
		result.plan =
		    make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_SEQUENCE, std::move(stmt.info), &schema);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &schema = BindCreateSchema(*stmt.info);
		result.plan =
		    make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_MACRO, std::move(stmt.info), &schema);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto &schema = BindCreateFunctionInfo(*stmt.info);
		result.plan =
		    make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_MACRO, std::move(stmt.info), &schema);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &base = stmt.info->Cast<CreateIndexInfo>();

		// visit the table reference
		auto table_ref = make_uniq<BaseTableRef>();
		table_ref->catalog_name = base.catalog;
		table_ref->schema_name = base.schema;
		table_ref->table_name = base.table;

		auto bound_table = Bind(*table_ref);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw BinderException("Can only create an index over a base table!");
		}
		auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
		auto &table = table_binding.table;
		if (table.temporary) {
			stmt.info->temporary = true;
		}
		// create a plan over the bound table
		auto plan = CreatePlan(*bound_table);
		if (plan->type != LogicalOperatorType::LOGICAL_GET) {
			throw BinderException("Cannot create index on a view!");
		}
		result.plan = table.catalog.BindCreateIndex(*this, stmt, table, std::move(plan));
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &create_info = stmt.info->Cast<CreateTableInfo>();
		// If there is a foreign key constraint, resolve primary key column's index from primary key column's name
		reference_set_t<SchemaCatalogEntry> fk_schemas;
		for (idx_t i = 0; i < create_info.constraints.size(); i++) {
			auto &cond = create_info.constraints[i];
			if (cond->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &fk = cond->Cast<ForeignKeyConstraint>();
			if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				continue;
			}
			D_ASSERT(fk.info.pk_keys.empty());
			D_ASSERT(fk.info.fk_keys.empty());
			FindForeignKeyIndexes(create_info.columns, fk.fk_columns, fk.info.fk_keys);
			if (StringUtil::CIEquals(create_info.table, fk.info.table)) {
				// self-referential foreign key constraint
				fk.info.type = ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE;
				FindMatchingPrimaryKeyColumns(create_info.columns, create_info.constraints, fk);
				FindForeignKeyIndexes(create_info.columns, fk.pk_columns, fk.info.pk_keys);
				CheckForeignKeyTypes(create_info.columns, create_info.columns, fk);
			} else {
				// have to resolve referenced table
				auto &pk_table_entry_ptr =
				    Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, fk.info.schema, fk.info.table);
				fk_schemas.insert(pk_table_entry_ptr.schema);
				FindMatchingPrimaryKeyColumns(pk_table_entry_ptr.GetColumns(), pk_table_entry_ptr.GetConstraints(), fk);
				FindForeignKeyIndexes(pk_table_entry_ptr.GetColumns(), fk.pk_columns, fk.info.pk_keys);
				CheckForeignKeyTypes(pk_table_entry_ptr.GetColumns(), create_info.columns, fk);
				auto &storage = pk_table_entry_ptr.GetStorage();
				auto index = storage.info->indexes.FindForeignKeyIndex(fk.info.pk_keys,
				                                                       ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE);
				if (!index) {
					auto fk_column_names = StringUtil::Join(fk.pk_columns, ",");
					throw BinderException("Failed to create foreign key on %s(%s): no UNIQUE or PRIMARY KEY constraint "
					                      "present on these columns",
					                      pk_table_entry_ptr.name, fk_column_names);
				}
			}
			D_ASSERT(fk.info.pk_keys.size() == fk.info.fk_keys.size());
			D_ASSERT(fk.info.pk_keys.size() == fk.pk_columns.size());
			D_ASSERT(fk.info.fk_keys.size() == fk.fk_columns.size());
		}
		if (AnyConstraintReferencesGeneratedColumn(create_info)) {
			throw BinderException("Constraints on generated columns are not supported yet");
		}
		auto bound_info = BindCreateTableInfo(std::move(stmt.info));
		auto root = std::move(bound_info->query);
		for (auto &fk_schema : fk_schemas) {
			if (&fk_schema.get() != &bound_info->schema) {
				throw BinderException("Creating foreign keys across different schemas or catalogs is not supported");
			}
		}

		// create the logical operator
		auto &schema = bound_info->schema;
		auto create_table = make_uniq<LogicalCreateTable>(schema, std::move(bound_info));
		if (root) {
			// CREATE TABLE AS
			properties.return_type = StatementReturnType::CHANGED_ROWS;
			create_table->children.push_back(std::move(root));
		}
		result.plan = std::move(create_table);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		auto &schema = BindCreateSchema(*stmt.info);
		auto &create_type_info = stmt.info->Cast<CreateTypeInfo>();
		result.plan = make_uniq<LogicalCreate>(LogicalOperatorType::LOGICAL_CREATE_TYPE, std::move(stmt.info), &schema);
		if (create_type_info.query) {
			// CREATE TYPE mood AS ENUM (SELECT 'happy')
			auto query_obj = Bind(*create_type_info.query);
			auto query = std::move(query_obj.plan);
			create_type_info.query.reset();

			auto &sql_types = query_obj.types;
			if (sql_types.size() != 1) {
				// add cast expression?
				throw BinderException("The query must return a single column");
			}
			if (sql_types[0].id() != LogicalType::VARCHAR) {
				// push a projection casting to varchar
				vector<unique_ptr<Expression>> select_list;
				auto ref = make_uniq<BoundColumnRefExpression>(sql_types[0], query->GetColumnBindings()[0]);
				auto cast_expr = BoundCastExpression::AddCastToType(context, std::move(ref), LogicalType::VARCHAR);
				select_list.push_back(std::move(cast_expr));
				auto proj = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(select_list));
				proj->AddChild(std::move(query));
				query = std::move(proj);
			}

			result.plan->AddChild(std::move(query));
		} else if (create_type_info.type.id() == LogicalTypeId::USER) {
			// two cases:
			// 1: create a type with a non-existant type as source, catalog.GetType(...) will throw exception.
			// 2: create a type alias with a custom type.
			// eg. CREATE TYPE a AS INT; CREATE TYPE b AS a;
			// We set b to be an alias for the underlying type of a
			auto inner_type = Catalog::GetType(context, schema.catalog.GetName(), schema.name,
			                                   UserType::GetTypeName(create_type_info.type));
			inner_type.SetAlias(create_type_info.name);
			create_type_info.type = inner_type;
		}
		break;
	}
	default:
		throw Exception("Unrecognized type!");
	}
	properties.return_type = StatementReturnType::NOTHING;
	properties.allow_stream_result = false;
	return result;
}

} // namespace duckdb






















#include <algorithm>

namespace duckdb {

static void CreateColumnDependencyManager(BoundCreateTableInfo &info) {
	auto &base = info.base->Cast<CreateTableInfo>();
	for (auto &col : base.columns.Logical()) {
		if (!col.Generated()) {
			continue;
		}
		info.column_dependency_manager.AddGeneratedColumn(col, base.columns);
	}
}

static void BindCheckConstraint(Binder &binder, BoundCreateTableInfo &info, const unique_ptr<Constraint> &cond) {
	auto &base = info.base->Cast<CreateTableInfo>();

	auto bound_constraint = make_uniq<BoundCheckConstraint>();
	// check constraint: bind the expression
	CheckBinder check_binder(binder, binder.context, base.table, base.columns, bound_constraint->bound_columns);
	auto &check = cond->Cast<CheckConstraint>();
	// create a copy of the unbound expression because the binding destroys the constraint
	auto unbound_expression = check.expression->Copy();
	// now bind the constraint and create a new BoundCheckConstraint
	bound_constraint->expression = check_binder.Bind(check.expression);
	info.bound_constraints.push_back(std::move(bound_constraint));
	// move the unbound constraint back into the original check expression
	check.expression = std::move(unbound_expression);
}

static void BindConstraints(Binder &binder, BoundCreateTableInfo &info) {
	auto &base = info.base->Cast<CreateTableInfo>();

	bool has_primary_key = false;
	logical_index_set_t not_null_columns;
	vector<LogicalIndex> primary_keys;
	for (idx_t i = 0; i < base.constraints.size(); i++) {
		auto &cond = base.constraints[i];
		switch (cond->type) {
		case ConstraintType::CHECK: {
			BindCheckConstraint(binder, info, cond);
			break;
		}
		case ConstraintType::NOT_NULL: {
			auto &not_null = cond->Cast<NotNullConstraint>();
			auto &col = base.columns.GetColumn(LogicalIndex(not_null.index));
			info.bound_constraints.push_back(make_uniq<BoundNotNullConstraint>(PhysicalIndex(col.StorageOid())));
			not_null_columns.insert(not_null.index);
			break;
		}
		case ConstraintType::UNIQUE: {
			auto &unique = cond->Cast<UniqueConstraint>();
			// have to resolve columns of the unique constraint
			vector<LogicalIndex> keys;
			logical_index_set_t key_set;
			if (unique.index.index != DConstants::INVALID_INDEX) {
				D_ASSERT(unique.index.index < base.columns.LogicalColumnCount());
				// unique constraint is given by single index
				unique.columns.push_back(base.columns.GetColumn(unique.index).Name());
				keys.push_back(unique.index);
				key_set.insert(unique.index);
			} else {
				// unique constraint is given by list of names
				// have to resolve names
				D_ASSERT(!unique.columns.empty());
				for (auto &keyname : unique.columns) {
					if (!base.columns.ColumnExists(keyname)) {
						throw ParserException("column \"%s\" named in key does not exist", keyname);
					}
					auto &column = base.columns.GetColumn(keyname);
					auto column_index = column.Logical();
					if (key_set.find(column_index) != key_set.end()) {
						throw ParserException("column \"%s\" appears twice in "
						                      "primary key constraint",
						                      keyname);
					}
					keys.push_back(column_index);
					key_set.insert(column_index);
				}
			}

			if (unique.is_primary_key) {
				// we can only have one primary key per table
				if (has_primary_key) {
					throw ParserException("table \"%s\" has more than one primary key", base.table);
				}
				has_primary_key = true;
				primary_keys = keys;
			}
			info.bound_constraints.push_back(
			    make_uniq<BoundUniqueConstraint>(std::move(keys), std::move(key_set), unique.is_primary_key));
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &fk = cond->Cast<ForeignKeyConstraint>();
			D_ASSERT((fk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE && !fk.info.pk_keys.empty()) ||
			         (fk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE && !fk.info.pk_keys.empty()) ||
			         fk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE);
			physical_index_set_t fk_key_set, pk_key_set;
			for (idx_t i = 0; i < fk.info.pk_keys.size(); i++) {
				if (pk_key_set.find(fk.info.pk_keys[i]) != pk_key_set.end()) {
					throw BinderException("Duplicate primary key referenced in FOREIGN KEY constraint");
				}
				pk_key_set.insert(fk.info.pk_keys[i]);
			}
			for (idx_t i = 0; i < fk.info.fk_keys.size(); i++) {
				if (fk_key_set.find(fk.info.fk_keys[i]) != fk_key_set.end()) {
					throw BinderException("Duplicate key specified in FOREIGN KEY constraint");
				}
				fk_key_set.insert(fk.info.fk_keys[i]);
			}
			info.bound_constraints.push_back(
			    make_uniq<BoundForeignKeyConstraint>(fk.info, std::move(pk_key_set), std::move(fk_key_set)));
			break;
		}
		default:
			throw NotImplementedException("unrecognized constraint type in bind");
		}
	}
	if (has_primary_key) {
		// if there is a primary key index, also create a NOT NULL constraint for each of the columns
		for (auto &column_index : primary_keys) {
			if (not_null_columns.count(column_index)) {
				//! No need to create a NotNullConstraint, it's already present
				continue;
			}
			auto physical_index = base.columns.LogicalToPhysical(column_index);
			base.constraints.push_back(make_uniq<NotNullConstraint>(column_index));
			info.bound_constraints.push_back(make_uniq<BoundNotNullConstraint>(physical_index));
		}
	}
}

void Binder::BindGeneratedColumns(BoundCreateTableInfo &info) {
	auto &base = info.base->Cast<CreateTableInfo>();

	vector<string> names;
	vector<LogicalType> types;

	D_ASSERT(base.type == CatalogType::TABLE_ENTRY);
	for (auto &col : base.columns.Logical()) {
		names.push_back(col.Name());
		types.push_back(col.Type());
	}
	auto table_index = GenerateTableIndex();

	// Create a new binder because we dont need (or want) these bindings in this scope
	auto binder = Binder::CreateBinder(context);
	binder->bind_context.AddGenericBinding(table_index, base.table, names, types);
	auto expr_binder = ExpressionBinder(*binder, context);
	string ignore;
	auto table_binding = binder->bind_context.GetBinding(base.table, ignore);
	D_ASSERT(table_binding && ignore.empty());

	auto bind_order = info.column_dependency_manager.GetBindOrder(base.columns);
	logical_index_set_t bound_indices;

	while (!bind_order.empty()) {
		auto i = bind_order.top();
		bind_order.pop();
		auto &col = base.columns.GetColumnMutable(i);

		//! Already bound this previously
		//! This can not be optimized out of the GetBindOrder function
		//! These occurrences happen because we need to make sure that ALL dependencies of a column are resolved before
		//! it gets resolved
		if (bound_indices.count(i)) {
			continue;
		}
		D_ASSERT(col.Generated());
		auto expression = col.GeneratedExpression().Copy();

		auto bound_expression = expr_binder.Bind(expression);
		D_ASSERT(bound_expression);
		D_ASSERT(!bound_expression->HasSubquery());
		if (col.Type().id() == LogicalTypeId::ANY) {
			// Do this before changing the type, so we know it's the first time the type is set
			col.ChangeGeneratedExpressionType(bound_expression->return_type);
			col.SetType(bound_expression->return_type);

			// Update the type in the binding, for future expansions
			string ignore;
			table_binding->types[i.index] = col.Type();
		}
		bound_indices.insert(i);
	}
}

void Binder::BindDefaultValues(const ColumnList &columns, vector<unique_ptr<Expression>> &bound_defaults) {
	for (auto &column : columns.Physical()) {
		unique_ptr<Expression> bound_default;
		if (column.DefaultValue()) {
			// we bind a copy of the DEFAULT value because binding is destructive
			// and we want to keep the original expression around for serialization
			auto default_copy = column.DefaultValue()->Copy();
			ConstantBinder default_binder(*this, context, "DEFAULT value");
			default_binder.target_type = column.Type();
			bound_default = default_binder.Bind(default_copy);
		} else {
			// no default value specified: push a default value of constant null
			bound_default = make_uniq<BoundConstantExpression>(Value(column.Type()));
		}
		bound_defaults.push_back(std::move(bound_default));
	}
}

static void ExtractExpressionDependencies(Expression &expr, DependencyList &dependencies) {
	if (expr.type == ExpressionType::BOUND_FUNCTION) {
		auto &function = expr.Cast<BoundFunctionExpression>();
		if (function.function.dependency) {
			function.function.dependency(function, dependencies);
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ExtractExpressionDependencies(child, dependencies); });
}

static void ExtractDependencies(BoundCreateTableInfo &info) {
	for (auto &default_value : info.bound_defaults) {
		if (default_value) {
			ExtractExpressionDependencies(*default_value, info.dependencies);
		}
	}
	for (auto &constraint : info.bound_constraints) {
		if (constraint->type == ConstraintType::CHECK) {
			auto &bound_check = constraint->Cast<BoundCheckConstraint>();
			ExtractExpressionDependencies(*bound_check.expression, info.dependencies);
		}
	}
}
unique_ptr<BoundCreateTableInfo> Binder::BindCreateTableInfo(unique_ptr<CreateInfo> info, SchemaCatalogEntry &schema) {
	auto &base = info->Cast<CreateTableInfo>();
	auto result = make_uniq<BoundCreateTableInfo>(schema, std::move(info));
	if (base.query) {
		// construct the result object
		auto query_obj = Bind(*base.query);
		base.query.reset();
		result->query = std::move(query_obj.plan);

		// construct the set of columns based on the names and types of the query
		auto &names = query_obj.names;
		auto &sql_types = query_obj.types;
		D_ASSERT(names.size() == sql_types.size());
		base.columns.SetAllowDuplicates(true);
		for (idx_t i = 0; i < names.size(); i++) {
			base.columns.AddColumn(ColumnDefinition(names[i], sql_types[i]));
		}
		CreateColumnDependencyManager(*result);
		// bind the generated column expressions
		BindGeneratedColumns(*result);
	} else {
		CreateColumnDependencyManager(*result);
		// bind the generated column expressions
		BindGeneratedColumns(*result);
		// bind any constraints
		BindConstraints(*this, *result);
		// bind the default values
		BindDefaultValues(base.columns, result->bound_defaults);
	}
	// extract dependencies from any default values or CHECK constraints
	ExtractDependencies(*result);

	if (base.columns.PhysicalColumnCount() == 0) {
		throw BinderException("Creating a table without physical (non-generated) columns is not supported");
	}
	// bind collations to detect any unsupported collation errors
	for (idx_t i = 0; i < base.columns.PhysicalColumnCount(); i++) {
		auto &column = base.columns.GetColumnMutable(PhysicalIndex(i));
		if (column.Type().id() == LogicalTypeId::VARCHAR) {
			ExpressionBinder::TestCollation(context, StringType::GetCollation(column.Type()));
		}
		BindLogicalType(context, column.TypeMutable(), &result->schema.catalog);
	}
	result->dependencies.VerifyDependencies(schema.catalog, result->Base().table);
	properties.allow_stream_result = false;
	return result;
}

unique_ptr<BoundCreateTableInfo> Binder::BindCreateTableInfo(unique_ptr<CreateInfo> info) {
	auto &base = info->Cast<CreateTableInfo>();
	auto &schema = BindCreateSchema(base);
	return BindCreateTableInfo(std::move(info), schema);
}

vector<unique_ptr<Expression>> Binder::BindCreateIndexExpressions(TableCatalogEntry &table, CreateIndexInfo &info) {
	auto index_binder = IndexBinder(*this, this->context, &table, &info);
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(info.expressions.size());
	for (auto &expr : info.expressions) {
		expressions.push_back(index_binder.Bind(expr));
	}

	return expressions;
}

} // namespace duckdb












namespace duckdb {

BoundStatement Binder::Bind(DeleteStatement &stmt) {
	BoundStatement result;

	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only delete from base table!");
	}
	auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
	auto &table = table_binding.table;

	auto root = CreatePlan(*bound_table);
	auto &get = root->Cast<LogicalGet>();
	D_ASSERT(root->type == LogicalOperatorType::LOGICAL_GET);

	if (!table.temporary) {
		// delete from persistent table: not read only!
		properties.modified_databases.insert(table.catalog.GetName());
	}

	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	// plan any tables from the various using clauses
	if (!stmt.using_clauses.empty()) {
		unique_ptr<LogicalOperator> child_operator;
		for (auto &using_clause : stmt.using_clauses) {
			// bind the using clause
			auto using_binder = Binder::CreateBinder(context, this);
			auto bound_node = using_binder->Bind(*using_clause);
			auto op = CreatePlan(*bound_node);
			if (child_operator) {
				// already bound a child: create a cross product to unify the two
				child_operator = LogicalCrossProduct::Create(std::move(child_operator), std::move(op));
			} else {
				child_operator = std::move(op);
			}
			bind_context.AddContext(std::move(using_binder->bind_context));
		}
		if (child_operator) {
			root = LogicalCrossProduct::Create(std::move(root), std::move(child_operator));
		}
	}

	// project any additional columns required for the condition
	unique_ptr<Expression> condition;
	if (stmt.condition) {
		WhereBinder binder(*this, context);
		condition = binder.Bind(stmt.condition);

		PlanSubqueries(condition, root);
		auto filter = make_uniq<LogicalFilter>(std::move(condition));
		filter->AddChild(std::move(root));
		root = std::move(filter);
	}
	// create the delete node
	auto del = make_uniq<LogicalDelete>(table, GenerateTableIndex());
	del->AddChild(std::move(root));

	// set up the delete expression
	del->expressions.push_back(make_uniq<BoundColumnRefExpression>(
	    LogicalType::ROW_TYPE, ColumnBinding(get.table_index, get.column_ids.size())));
	get.column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);

	if (!stmt.returning_list.empty()) {
		del->return_chunk = true;

		auto update_table_index = GenerateTableIndex();
		del->table_index = update_table_index;

		unique_ptr<LogicalOperator> del_as_logicaloperator = std::move(del);
		return BindReturning(std::move(stmt.returning_list), table, stmt.table->alias, update_table_index,
		                     std::move(del_as_logicaloperator), std::move(result));
	}
	result.plan = std::move(del);
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;

	return result;
}

} // namespace duckdb





namespace duckdb {

BoundStatement Binder::Bind(DetachStatement &stmt) {
	BoundStatement result;

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DETACH, std::move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb











namespace duckdb {

BoundStatement Binder::Bind(DropStatement &stmt) {
	BoundStatement result;

	switch (stmt.info->type) {
	case CatalogType::PREPARED_STATEMENT:
		// dropping prepared statements is always possible
		// it also does not require a valid transaction
		properties.requires_valid_transaction = false;
		break;
	case CatalogType::SCHEMA_ENTRY: {
		// dropping a schema is never read-only because there are no temporary schemas
		auto &catalog = Catalog::GetCatalog(context, stmt.info->catalog);
		properties.modified_databases.insert(catalog.GetName());
		break;
	}
	case CatalogType::VIEW_ENTRY:
	case CatalogType::SEQUENCE_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::TYPE_ENTRY: {
		BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
		auto entry = Catalog::GetEntry(context, stmt.info->type, stmt.info->catalog, stmt.info->schema, stmt.info->name,
		                               OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			break;
		}
		if (entry->internal) {
			throw CatalogException("Cannot drop internal catalog entry \"%s\"!", entry->name);
		}
		stmt.info->catalog = entry->ParentCatalog().GetName();
		if (!entry->temporary) {
			// we can only drop temporary tables in read-only mode
			properties.modified_databases.insert(stmt.info->catalog);
		}
		stmt.info->schema = entry->ParentSchema().name;
		break;
	}
	default:
		throw BinderException("Unknown catalog type for drop statement!");
	}
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DROP, std::move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb










namespace duckdb {

BoundStatement Binder::Bind(ExecuteStatement &stmt) {
	auto parameter_count = stmt.n_param;

	// bind the prepared statement
	auto &client_data = ClientData::Get(context);

	auto entry = client_data.prepared_statements.find(stmt.name);
	if (entry == client_data.prepared_statements.end()) {
		throw BinderException("Prepared statement \"%s\" does not exist", stmt.name);
	}

	// check if we need to rebind the prepared statement
	// this happens if the catalog changes, since in this case e.g. tables we relied on may have been deleted
	auto prepared = entry->second;
	auto &named_param_map = prepared->unbound_statement->named_param_map;

	PreparedStatement::VerifyParameters(stmt.named_values, named_param_map);

	auto &mapped_named_values = stmt.named_values;
	// bind any supplied parameters
	case_insensitive_map_t<Value> bind_values;
	auto constant_binder = Binder::CreateBinder(context);
	constant_binder->SetCanContainNulls(true);
	for (auto &pair : mapped_named_values) {
		ConstantBinder cbinder(*constant_binder, context, "EXECUTE statement");
		auto bound_expr = cbinder.Bind(pair.second);

		Value value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		bind_values[pair.first] = std::move(value);
	}
	unique_ptr<LogicalOperator> rebound_plan;

	if (prepared->RequireRebind(context, &bind_values)) {
		// catalog was modified or statement does not have clear types: rebind the statement before running the execute
		Planner prepared_planner(context);
		for (auto &pair : bind_values) {
			prepared_planner.parameter_data.emplace(std::make_pair(pair.first, BoundParameterData(pair.second)));
		}
		prepared = prepared_planner.PrepareSQLStatement(entry->second->unbound_statement->Copy());
		rebound_plan = std::move(prepared_planner.plan);
		D_ASSERT(prepared->properties.bound_all_parameters);
		this->bound_tables = prepared_planner.binder->bound_tables;
	}
	// copy the properties of the prepared statement into the planner
	this->properties = prepared->properties;
	this->properties.parameter_count = parameter_count;
	BoundStatement result;
	result.names = prepared->names;
	result.types = prepared->types;

	prepared->Bind(std::move(bind_values));
	if (rebound_plan) {
		auto execute_plan = make_uniq<LogicalExecute>(std::move(prepared));
		execute_plan->children.push_back(std::move(rebound_plan));
		result.plan = std::move(execute_plan);
	} else {
		result.plan = make_uniq<LogicalExecute>(std::move(prepared));
	}
	return result;
}

} // namespace duckdb




namespace duckdb {

BoundStatement Binder::Bind(ExplainStatement &stmt) {
	BoundStatement result;

	// bind the underlying statement
	auto plan = Bind(*stmt.stmt);
	// get the unoptimized logical plan, and create the explain statement
	auto logical_plan_unopt = plan.plan->ToString();
	auto explain = make_uniq<LogicalExplain>(std::move(plan.plan), stmt.explain_type);
	explain->logical_plan_unopt = logical_plan_unopt;

	result.plan = std::move(explain);
	result.names = {"explain_key", "explain_value"};
	result.types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb


















#include <algorithm>

namespace duckdb {

//! Sanitizes a string to have only low case chars and underscores
string SanitizeExportIdentifier(const string &str) {
	// Copy the original string to result
	string result(str);

	for (idx_t i = 0; i < str.length(); ++i) {
		auto c = str[i];
		if (c >= 'a' && c <= 'z') {
			// If it is lower case just continue
			continue;
		}

		if (c >= 'A' && c <= 'Z') {
			// To lowercase
			result[i] = tolower(c);
		} else {
			// Substitute to underscore
			result[i] = '_';
		}
	}

	return result;
}

bool ReferencedTableIsOrdered(string &referenced_table, catalog_entry_vector_t &ordered) {
	for (auto &entry : ordered) {
		auto &table_entry = entry.get().Cast<TableCatalogEntry>();
		if (StringUtil::CIEquals(table_entry.name, referenced_table)) {
			// The referenced table is already ordered
			return true;
		}
	}
	return false;
}

void ScanForeignKeyTable(catalog_entry_vector_t &ordered, catalog_entry_vector_t &unordered,
                         bool move_primary_keys_only) {
	catalog_entry_vector_t remaining;

	for (auto &entry : unordered) {
		auto &table_entry = entry.get().Cast<TableCatalogEntry>();
		bool move_to_ordered = true;
		auto &constraints = table_entry.GetConstraints();

		for (auto &cond : constraints) {
			if (cond->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &fk = cond->Cast<ForeignKeyConstraint>();
			if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				continue;
			}

			if (move_primary_keys_only) {
				// This table references a table, don't move it yet
				move_to_ordered = false;
				break;
			} else if (!ReferencedTableIsOrdered(fk.info.table, ordered)) {
				// The table that it references isn't ordered yet
				move_to_ordered = false;
				break;
			}
		}

		if (move_to_ordered) {
			ordered.push_back(table_entry);
		} else {
			remaining.push_back(table_entry);
		}
	}
	unordered = remaining;
}

void ReorderTableEntries(catalog_entry_vector_t &tables) {
	catalog_entry_vector_t ordered;
	catalog_entry_vector_t unordered = tables;
	// First only move the tables that don't have any dependencies
	ScanForeignKeyTable(ordered, unordered, true);
	while (!unordered.empty()) {
		// Now we will start moving tables that have foreign key constraints
		// if the tables they reference are already moved
		ScanForeignKeyTable(ordered, unordered, false);
	}
	tables = ordered;
}

string CreateFileName(const string &id_suffix, TableCatalogEntry &table, const string &extension) {
	auto name = SanitizeExportIdentifier(table.name);
	if (table.schema.name == DEFAULT_SCHEMA) {
		return StringUtil::Format("%s%s.%s", name, id_suffix, extension);
	}
	auto schema = SanitizeExportIdentifier(table.schema.name);
	return StringUtil::Format("%s_%s%s.%s", schema, name, id_suffix, extension);
}

static bool IsSupported(CopyTypeSupport support_level) {
	// For export purposes we don't want to lose information, so we only accept fully supported types
	return support_level == CopyTypeSupport::SUPPORTED;
}

static LogicalType AlterLogicalType(const LogicalType &original, copy_supports_type_t type_check) {
	D_ASSERT(type_check);
	auto id = original.id();
	switch (id) {
	case LogicalTypeId::LIST: {
		auto child = AlterLogicalType(ListType::GetChildType(original), type_check);
		return LogicalType::LIST(child);
	}
	case LogicalTypeId::STRUCT: {
		auto &original_children = StructType::GetChildTypes(original);
		child_list_t<LogicalType> new_children;
		for (auto &child : original_children) {
			auto &child_name = child.first;
			auto &child_type = child.second;

			LogicalType new_type;
			if (!IsSupported(type_check(child_type))) {
				new_type = AlterLogicalType(child_type, type_check);
			} else {
				new_type = child_type;
			}
			new_children.push_back(std::make_pair(child_name, new_type));
		}
		return LogicalType::STRUCT(std::move(new_children));
	}
	case LogicalTypeId::UNION: {
		auto member_count = UnionType::GetMemberCount(original);
		child_list_t<LogicalType> new_children;
		for (idx_t i = 0; i < member_count; i++) {
			auto &child_name = UnionType::GetMemberName(original, i);
			auto &child_type = UnionType::GetMemberType(original, i);

			LogicalType new_type;
			if (!IsSupported(type_check(child_type))) {
				new_type = AlterLogicalType(child_type, type_check);
			} else {
				new_type = child_type;
			}

			new_children.push_back(std::make_pair(child_name, new_type));
		}
		return LogicalType::UNION(std::move(new_children));
	}
	case LogicalTypeId::MAP: {
		auto &key_type = MapType::KeyType(original);
		auto &value_type = MapType::ValueType(original);

		LogicalType new_key_type;
		LogicalType new_value_type;
		if (!IsSupported(type_check(key_type))) {
			new_key_type = AlterLogicalType(key_type, type_check);
		} else {
			new_key_type = key_type;
		}

		if (!IsSupported(type_check(value_type))) {
			new_value_type = AlterLogicalType(value_type, type_check);
		} else {
			new_value_type = value_type;
		}
		return LogicalType::MAP(new_key_type, new_value_type);
	}
	default: {
		D_ASSERT(!IsSupported(type_check(original)));
		return LogicalType::VARCHAR;
	}
	}
}

static bool NeedsCast(LogicalType &type, copy_supports_type_t type_check) {
	if (!type_check) {
		return false;
	}
	if (IsSupported(type_check(type))) {
		// The type is supported in it's entirety, no cast is required
		return false;
	}
	// Change the type to something that is supported
	type = AlterLogicalType(type, type_check);
	return true;
}

static unique_ptr<QueryNode> CreateSelectStatement(CopyStatement &stmt, child_list_t<LogicalType> &select_list,
                                                   copy_supports_type_t type_check) {
	auto ref = make_uniq<BaseTableRef>();
	ref->catalog_name = stmt.info->catalog;
	ref->schema_name = stmt.info->schema;
	ref->table_name = stmt.info->table;

	auto statement = make_uniq<SelectNode>();
	statement->from_table = std::move(ref);

	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto &col : select_list) {
		auto &name = col.first;
		auto &type = col.second;

		auto expression = make_uniq_base<ParsedExpression, ColumnRefExpression>(name);
		if (NeedsCast(type, type_check)) {
			// Add a cast to a type supported by the copy function
			expression = make_uniq_base<ParsedExpression, CastExpression>(type, std::move(expression));
		}
		expressions.push_back(std::move(expression));
	}

	statement->select_list = std::move(expressions);
	return std::move(statement);
}

BoundStatement Binder::Bind(ExportStatement &stmt) {
	// COPY TO a file
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("COPY TO is disabled through configuration");
	}
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// lookup the format in the catalog
	auto &copy_function =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, stmt.info->format);
	if (!copy_function.function.copy_to_bind && !copy_function.function.plan) {
		throw NotImplementedException("COPY TO is not supported for FORMAT \"%s\"", stmt.info->format);
	}

	// gather a list of all the tables
	string catalog = stmt.database.empty() ? INVALID_CATALOG : stmt.database;
	catalog_entry_vector_t tables;
	auto schemas = Catalog::GetSchemas(context, catalog);
	for (auto &schema : schemas) {
		schema.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.type == CatalogType::TABLE_ENTRY) {
				tables.push_back(entry.Cast<TableCatalogEntry>());
			}
		});
	}

	// reorder tables because of foreign key constraint
	ReorderTableEntries(tables);

	// now generate the COPY statements for each of the tables
	auto &fs = FileSystem::GetFileSystem(context);
	unique_ptr<LogicalOperator> child_operator;

	BoundExportData exported_tables;

	unordered_set<string> table_name_index;
	for (auto &t : tables) {
		auto &table = t.get().Cast<TableCatalogEntry>();
		auto info = make_uniq<CopyInfo>();
		// we copy the options supplied to the EXPORT
		info->format = stmt.info->format;
		info->options = stmt.info->options;
		// set up the file name for the COPY TO

		idx_t id = 0;
		while (true) {
			string id_suffix = id == 0 ? string() : "_" + to_string(id);
			auto name = CreateFileName(id_suffix, table, copy_function.function.extension);
			auto directory = stmt.info->file_path;
			auto full_path = fs.JoinPath(directory, name);
			info->file_path = full_path;
			auto insert_result = table_name_index.insert(info->file_path);
			if (insert_result.second == true) {
				// this name was not yet taken: take it
				break;
			}
			id++;
		}
		info->is_from = false;
		info->catalog = catalog;
		info->schema = table.schema.name;
		info->table = table.name;

		// We can not export generated columns
		child_list_t<LogicalType> select_list;

		for (auto &col : table.GetColumns().Physical()) {
			select_list.push_back(std::make_pair(col.Name(), col.Type()));
		}

		ExportedTableData exported_data;
		exported_data.database_name = catalog;
		exported_data.table_name = info->table;
		exported_data.schema_name = info->schema;

		exported_data.file_path = info->file_path;

		ExportedTableInfo table_info(table, std::move(exported_data));
		exported_tables.data.push_back(table_info);
		id++;

		// generate the copy statement and bind it
		CopyStatement copy_stmt;
		copy_stmt.info = std::move(info);
		copy_stmt.select_statement =
		    CreateSelectStatement(copy_stmt, select_list, copy_function.function.supports_type);

		auto copy_binder = Binder::CreateBinder(context, this);
		auto bound_statement = copy_binder->Bind(copy_stmt);
		auto plan = std::move(bound_statement.plan);

		if (child_operator) {
			// use UNION ALL to combine the individual copy statements into a single node
			auto copy_union = make_uniq<LogicalSetOperation>(GenerateTableIndex(), 1, std::move(child_operator),
			                                                 std::move(plan), LogicalOperatorType::LOGICAL_UNION);
			child_operator = std::move(copy_union);
		} else {
			child_operator = std::move(plan);
		}
	}

	// try to create the directory, if it doesn't exist yet
	// a bit hacky to do it here, but we need to create the directory BEFORE the copy statements run
	if (!fs.DirectoryExists(stmt.info->file_path)) {
		fs.CreateDirectory(stmt.info->file_path);
	}

	// create the export node
	auto export_node = make_uniq<LogicalExport>(copy_function.function, std::move(stmt.info), exported_tables);

	if (child_operator) {
		export_node->children.push_back(std::move(child_operator));
	}

	result.plan = std::move(export_node);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb




namespace duckdb {

BoundStatement Binder::Bind(ExtensionStatement &stmt) {
	BoundStatement result;

	// perform the planning of the function
	D_ASSERT(stmt.extension.plan_function);
	auto parse_result =
	    stmt.extension.plan_function(stmt.extension.parser_info.get(), context, std::move(stmt.parse_data));

	properties.modified_databases = parse_result.modified_databases;
	properties.requires_valid_transaction = parse_result.requires_valid_transaction;
	properties.return_type = parse_result.return_type;

	// create the plan as a scan of the given table function
	result.plan = BindTableFunction(parse_result.function, std::move(parse_result.parameters));
	D_ASSERT(result.plan->type == LogicalOperatorType::LOGICAL_GET);
	auto &get = result.plan->Cast<LogicalGet>();
	result.names = get.names;
	result.types = get.returned_types;
	get.column_ids.clear();
	for (idx_t i = 0; i < get.returned_types.size(); i++) {
		get.column_ids.push_back(i);
	}
	return result;
}

} // namespace duckdb





























namespace duckdb {

static void CheckInsertColumnCountMismatch(int64_t expected_columns, int64_t result_columns, bool columns_provided,
                                           const char *tname) {
	if (result_columns != expected_columns) {
		string msg = StringUtil::Format(!columns_provided ? "table %s has %lld columns but %lld values were supplied"
		                                                  : "Column name/value mismatch for insert on %s: "
		                                                    "expected %lld columns but %lld values were supplied",
		                                tname, expected_columns, result_columns);
		throw BinderException(msg);
	}
}

unique_ptr<ParsedExpression> ExpandDefaultExpression(const ColumnDefinition &column) {
	if (column.DefaultValue()) {
		return column.DefaultValue()->Copy();
	} else {
		return make_uniq<ConstantExpression>(Value(column.Type()));
	}
}

void ReplaceDefaultExpression(unique_ptr<ParsedExpression> &expr, const ColumnDefinition &column) {
	D_ASSERT(expr->type == ExpressionType::VALUE_DEFAULT);
	expr = ExpandDefaultExpression(column);
}

void QualifyColumnReferences(unique_ptr<ParsedExpression> &expr, const string &table_name) {
	// To avoid ambiguity with 'excluded', we explicitly qualify all column references
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto &column_ref = expr->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			return;
		}
		auto column_name = column_ref.GetColumnName();
		expr = make_uniq<ColumnRefExpression>(column_name, table_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    *expr, [&](unique_ptr<ParsedExpression> &child) { QualifyColumnReferences(child, table_name); });
}

// Replace binding.table_index with 'dest' if it's 'source'
void ReplaceColumnBindings(Expression &expr, idx_t source, idx_t dest) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_columnref = expr.Cast<BoundColumnRefExpression>();
		if (bound_columnref.binding.table_index == source) {
			bound_columnref.binding.table_index = dest;
		}
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](unique_ptr<Expression> &child) { ReplaceColumnBindings(*child, source, dest); });
}

void Binder::BindDoUpdateSetExpressions(const string &table_alias, LogicalInsert &insert, UpdateSetInfo &set_info,
                                        TableCatalogEntry &table, TableStorageInfo &storage_info) {
	D_ASSERT(insert.children.size() == 1);
	D_ASSERT(insert.children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION);

	vector<column_t> logical_column_ids;
	vector<string> column_names;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	for (idx_t i = 0; i < set_info.columns.size(); i++) {
		auto &colname = set_info.columns[i];
		auto &expr = set_info.expressions[i];
		if (!table.ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname);
		}
		auto &column = table.GetColumn(colname);
		if (column.Generated()) {
			throw BinderException("Cant update column \"%s\" because it is a generated column!", column.Name());
		}
		if (std::find(insert.set_columns.begin(), insert.set_columns.end(), column.Physical()) !=
		    insert.set_columns.end()) {
			throw BinderException("Multiple assignments to same column \"%s\"", colname);
		}
		insert.set_columns.push_back(column.Physical());
		logical_column_ids.push_back(column.Oid());
		insert.set_types.push_back(column.Type());
		column_names.push_back(colname);
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			expr = ExpandDefaultExpression(column);
		}
		UpdateBinder binder(*this, context);
		binder.target_type = column.Type();

		// Avoid ambiguity issues
		QualifyColumnReferences(expr, table_alias);

		auto bound_expr = binder.Bind(expr);
		D_ASSERT(bound_expr);
		if (bound_expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
			throw BinderException("Expression in the DO UPDATE SET clause can not be a subquery");
		}

		insert.expressions.push_back(std::move(bound_expr));
	}

	// Figure out which columns are indexed on
	unordered_set<column_t> indexed_columns;
	for (auto &index : storage_info.index_info) {
		for (auto &column_id : index.column_set) {
			indexed_columns.insert(column_id);
		}
	}

	// Verify that none of the columns that are targeted with a SET expression are indexed on
	for (idx_t i = 0; i < logical_column_ids.size(); i++) {
		auto &column = logical_column_ids[i];
		if (indexed_columns.count(column)) {
			throw BinderException("Can not assign to column '%s' because it has a UNIQUE/PRIMARY KEY constraint",
			                      column_names[i]);
		}
	}
}

unique_ptr<UpdateSetInfo> CreateSetInfoForReplace(TableCatalogEntry &table, InsertStatement &insert,
                                                  TableStorageInfo &storage_info) {
	auto set_info = make_uniq<UpdateSetInfo>();

	auto &columns = set_info->columns;
	// Figure out which columns are indexed on

	unordered_set<column_t> indexed_columns;
	for (auto &index : storage_info.index_info) {
		for (auto &column_id : index.column_set) {
			indexed_columns.insert(column_id);
		}
	}

	auto &column_list = table.GetColumns();
	if (insert.columns.empty()) {
		for (auto &column : column_list.Physical()) {
			auto &name = column.Name();
			// FIXME: can these column names be aliased somehow?
			if (indexed_columns.count(column.Oid())) {
				continue;
			}
			columns.push_back(name);
		}
	} else {
		// a list of columns was explicitly supplied, only update those
		for (auto &name : insert.columns) {
			auto &column = column_list.GetColumn(name);
			if (indexed_columns.count(column.Oid())) {
				continue;
			}
			columns.push_back(name);
		}
	}

	// Create 'excluded' qualified column references of these columns
	for (auto &column : columns) {
		set_info->expressions.push_back(make_uniq<ColumnRefExpression>(column, "excluded"));
	}

	return set_info;
}

void Binder::BindOnConflictClause(LogicalInsert &insert, TableCatalogEntry &table, InsertStatement &stmt) {
	if (!stmt.on_conflict_info) {
		insert.action_type = OnConflictAction::THROW;
		return;
	}
	D_ASSERT(stmt.table_ref->type == TableReferenceType::BASE_TABLE);

	// visit the table reference
	auto bound_table = Bind(*stmt.table_ref);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}

	auto &table_ref = stmt.table_ref->Cast<BaseTableRef>();
	const string &table_alias = !table_ref.alias.empty() ? table_ref.alias : table_ref.table_name;

	auto &on_conflict = *stmt.on_conflict_info;
	D_ASSERT(on_conflict.action_type != OnConflictAction::THROW);
	insert.action_type = on_conflict.action_type;

	// obtain the table storage info
	auto storage_info = table.GetStorageInfo(context);

	auto &columns = table.GetColumns();
	if (!on_conflict.indexed_columns.empty()) {
		// Bind the ON CONFLICT (<columns>)

		// create a mapping of (list index) -> (column index)
		case_insensitive_map_t<idx_t> specified_columns;
		for (idx_t i = 0; i < on_conflict.indexed_columns.size(); i++) {
			specified_columns[on_conflict.indexed_columns[i]] = i;
			auto column_index = table.GetColumnIndex(on_conflict.indexed_columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot specify ROWID as ON CONFLICT target");
			}
			auto &col = columns.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot specify a generated column as ON CONFLICT target");
			}
		}
		for (auto &col : columns.Physical()) {
			auto entry = specified_columns.find(col.Name());
			if (entry != specified_columns.end()) {
				// column was specified, set to the index
				insert.on_conflict_filter.insert(col.Oid());
			}
		}
		bool index_references_columns = false;
		for (auto &index : storage_info.index_info) {
			if (!index.is_unique) {
				continue;
			}
			bool index_matches = insert.on_conflict_filter == index.column_set;
			if (index_matches) {
				index_references_columns = true;
				break;
			}
		}
		if (!index_references_columns) {
			// Same as before, this is essentially a no-op, turning this into a DO THROW instead
			// But since this makes no logical sense, it's probably better to throw an error
			throw BinderException(
			    "The specified columns as conflict target are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT");
		}
	} else {
		// When omitting the conflict target, the ON CONFLICT applies to every UNIQUE/PRIMARY KEY on the table

		// We check if there are any constraints on the table, if there aren't we throw an error.
		idx_t found_matching_indexes = 0;
		for (auto &index : storage_info.index_info) {
			if (!index.is_unique) {
				continue;
			}
			// does this work with multi-column indexes?
			auto &indexed_columns = index.column_set;
			for (auto &column : table.GetColumns().Physical()) {
				if (indexed_columns.count(column.Physical().index)) {
					found_matching_indexes++;
				}
			}
		}
		if (!found_matching_indexes) {
			throw BinderException(
			    "There are no UNIQUE/PRIMARY KEY Indexes that refer to this table, ON CONFLICT is a no-op");
		}
		if (insert.action_type != OnConflictAction::NOTHING && found_matching_indexes != 1) {
			// When no conflict target is provided, and the action type is UPDATE,
			// we only allow the operation when only a single Index exists
			throw BinderException("Conflict target has to be provided for a DO UPDATE operation when the table has "
			                      "multiple UNIQUE/PRIMARY KEY constraints");
		}
	}

	// add the 'excluded' dummy table binding
	AddTableName("excluded");
	// add a bind context entry for it
	auto excluded_index = GenerateTableIndex();
	insert.excluded_table_index = excluded_index;
	auto table_column_names = columns.GetColumnNames();
	auto table_column_types = columns.GetColumnTypes();
	bind_context.AddGenericBinding(excluded_index, "excluded", table_column_names, table_column_types);

	if (on_conflict.condition) {
		// Avoid ambiguity between <table_name> binding and 'excluded'
		QualifyColumnReferences(on_conflict.condition, table_alias);
		// Bind the ON CONFLICT ... WHERE clause
		WhereBinder where_binder(*this, context);
		auto condition = where_binder.Bind(on_conflict.condition);
		if (condition && condition->expression_class == ExpressionClass::BOUND_SUBQUERY) {
			throw BinderException("conflict_target WHERE clause can not be a subquery");
		}
		insert.on_conflict_condition = std::move(condition);
	}

	auto bindings = insert.children[0]->GetColumnBindings();
	idx_t projection_index = DConstants::INVALID_INDEX;
	vector<unique_ptr<LogicalOperator>> *insert_child_operators;
	insert_child_operators = &insert.children;
	while (projection_index == DConstants::INVALID_INDEX) {
		if (insert_child_operators->empty()) {
			// No further children to visit
			break;
		}
		D_ASSERT(insert_child_operators->size() >= 1);
		auto &current_child = (*insert_child_operators)[0];
		auto table_indices = current_child->GetTableIndex();
		if (table_indices.empty()) {
			// This operator does not have a table index to refer to, we have to visit its children
			insert_child_operators = &current_child->children;
			continue;
		}
		projection_index = table_indices[0];
	}
	if (projection_index == DConstants::INVALID_INDEX) {
		throw InternalException("Could not locate a table_index from the children of the insert");
	}

	string unused;
	auto original_binding = bind_context.GetBinding(table_alias, unused);
	D_ASSERT(original_binding);

	auto table_index = original_binding->index;

	// Replace any column bindings to refer to the projection table_index, rather than the source table
	if (insert.on_conflict_condition) {
		ReplaceColumnBindings(*insert.on_conflict_condition, table_index, projection_index);
	}

	if (insert.action_type == OnConflictAction::REPLACE) {
		D_ASSERT(on_conflict.set_info == nullptr);
		on_conflict.set_info = CreateSetInfoForReplace(table, stmt, storage_info);
		insert.action_type = OnConflictAction::UPDATE;
	}
	if (on_conflict.set_info && on_conflict.set_info->columns.empty()) {
		// if we are doing INSERT OR REPLACE on a table with no columns outside of the primary key column
		// convert to INSERT OR IGNORE
		insert.action_type = OnConflictAction::NOTHING;
	}
	if (insert.action_type == OnConflictAction::NOTHING) {
		if (!insert.on_conflict_condition) {
			return;
		}
		// Get the column_ids we need to fetch later on from the conflicting tuples
		// of the original table, to execute the expressions
		D_ASSERT(original_binding->binding_type == BindingType::TABLE);
		auto &table_binding = original_binding->Cast<TableBinding>();
		insert.columns_to_fetch = table_binding.GetBoundColumnIds();
		return;
	}

	D_ASSERT(on_conflict.set_info);
	auto &set_info = *on_conflict.set_info;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());

	if (set_info.condition) {
		// Avoid ambiguity between <table_name> binding and 'excluded'
		QualifyColumnReferences(set_info.condition, table_alias);
		// Bind the SET ... WHERE clause
		WhereBinder where_binder(*this, context);
		auto condition = where_binder.Bind(set_info.condition);
		if (condition && condition->expression_class == ExpressionClass::BOUND_SUBQUERY) {
			throw BinderException("conflict_target WHERE clause can not be a subquery");
		}
		insert.do_update_condition = std::move(condition);
	}

	BindDoUpdateSetExpressions(table_alias, insert, set_info, table, storage_info);

	// Get the column_ids we need to fetch later on from the conflicting tuples
	// of the original table, to execute the expressions
	D_ASSERT(original_binding->binding_type == BindingType::TABLE);
	auto &table_binding = original_binding->Cast<TableBinding>();
	insert.columns_to_fetch = table_binding.GetBoundColumnIds();

	// Replace the column bindings to refer to the child operator
	for (auto &expr : insert.expressions) {
		// Change the non-excluded column references to refer to the projection index
		ReplaceColumnBindings(*expr, table_index, projection_index);
	}
	// Do the same for the (optional) DO UPDATE condition
	if (insert.do_update_condition) {
		ReplaceColumnBindings(*insert.do_update_condition, table_index, projection_index);
	}
}

BoundStatement Binder::Bind(InsertStatement &stmt) {
	BoundStatement result;
	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};

	BindSchemaOrCatalog(stmt.catalog, stmt.schema);
	auto &table = Catalog::GetEntry<TableCatalogEntry>(context, stmt.catalog, stmt.schema, stmt.table);
	if (!table.temporary) {
		// inserting into a non-temporary table: alters underlying database
		properties.modified_databases.insert(table.catalog.GetName());
	}

	auto insert = make_uniq<LogicalInsert>(table, GenerateTableIndex());
	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	auto values_list = stmt.GetValuesList();

	// bind the root select node (if any)
	BoundStatement root_select;
	if (stmt.column_order == InsertColumnOrder::INSERT_BY_NAME) {
		if (values_list) {
			throw BinderException("INSERT BY NAME can only be used when inserting from a SELECT statement");
		}
		if (!stmt.columns.empty()) {
			throw BinderException("INSERT BY NAME cannot be combined with an explicit column list");
		}
		D_ASSERT(stmt.select_statement);
		// INSERT BY NAME - generate the columns from the names of the SELECT statement
		auto select_binder = Binder::CreateBinder(context, this);
		root_select = select_binder->Bind(*stmt.select_statement);
		MoveCorrelatedExpressions(*select_binder);

		stmt.columns = root_select.names;
	}

	vector<LogicalIndex> named_column_map;
	if (!stmt.columns.empty() || stmt.default_values) {
		// insertion statement specifies column list

		// create a mapping of (list index) -> (column index)
		case_insensitive_map_t<idx_t> column_name_map;
		for (idx_t i = 0; i < stmt.columns.size(); i++) {
			auto entry = column_name_map.insert(make_pair(stmt.columns[i], i));
			if (!entry.second) {
				throw BinderException("Duplicate column name \"%s\" in INSERT", stmt.columns[i]);
			}
			column_name_map[stmt.columns[i]] = i;
			auto column_index = table.GetColumnIndex(stmt.columns[i]);
			if (column_index.index == COLUMN_IDENTIFIER_ROW_ID) {
				throw BinderException("Cannot explicitly insert values into rowid column");
			}
			auto &col = table.GetColumn(column_index);
			if (col.Generated()) {
				throw BinderException("Cannot insert into a generated column");
			}
			insert->expected_types.push_back(col.Type());
			named_column_map.push_back(column_index);
		}
		for (auto &col : table.GetColumns().Physical()) {
			auto entry = column_name_map.find(col.Name());
			if (entry == column_name_map.end()) {
				// column not specified, set index to DConstants::INVALID_INDEX
				insert->column_index_map.push_back(DConstants::INVALID_INDEX);
			} else {
				// column was specified, set to the index
				insert->column_index_map.push_back(entry->second);
			}
		}
	} else {
		// insert by position and no columns specified - insertion into all columns of the table
		// intentionally don't populate 'column_index_map' as an indication of this
		for (auto &col : table.GetColumns().Physical()) {
			named_column_map.push_back(col.Logical());
			insert->expected_types.push_back(col.Type());
		}
	}

	// bind the default values
	BindDefaultValues(table.GetColumns(), insert->bound_defaults);
	if (!stmt.select_statement && !stmt.default_values) {
		result.plan = std::move(insert);
		return result;
	}
	// Exclude the generated columns from this amount
	idx_t expected_columns = stmt.columns.empty() ? table.GetColumns().PhysicalColumnCount() : stmt.columns.size();

	// special case: check if we are inserting from a VALUES statement
	if (values_list) {
		auto &expr_list = values_list->Cast<ExpressionListRef>();
		expr_list.expected_types.resize(expected_columns);
		expr_list.expected_names.resize(expected_columns);

		D_ASSERT(expr_list.values.size() > 0);
		CheckInsertColumnCountMismatch(expected_columns, expr_list.values[0].size(), !stmt.columns.empty(),
		                               table.name.c_str());

		// VALUES list!
		for (idx_t col_idx = 0; col_idx < expected_columns; col_idx++) {
			D_ASSERT(named_column_map.size() >= col_idx);
			auto &table_col_idx = named_column_map[col_idx];

			// set the expected types as the types for the INSERT statement
			auto &column = table.GetColumn(table_col_idx);
			expr_list.expected_types[col_idx] = column.Type();
			expr_list.expected_names[col_idx] = column.Name();

			// now replace any DEFAULT values with the corresponding default expression
			for (idx_t list_idx = 0; list_idx < expr_list.values.size(); list_idx++) {
				if (expr_list.values[list_idx][col_idx]->type == ExpressionType::VALUE_DEFAULT) {
					// DEFAULT value! replace the entry
					ReplaceDefaultExpression(expr_list.values[list_idx][col_idx], column);
				}
			}
		}
	}

	// parse select statement and add to logical plan
	unique_ptr<LogicalOperator> root;
	if (stmt.select_statement) {
		if (stmt.column_order == InsertColumnOrder::INSERT_BY_POSITION) {
			auto select_binder = Binder::CreateBinder(context, this);
			root_select = select_binder->Bind(*stmt.select_statement);
			MoveCorrelatedExpressions(*select_binder);
		}
		// inserting from a select - check if the column count matches
		CheckInsertColumnCountMismatch(expected_columns, root_select.types.size(), !stmt.columns.empty(),
		                               table.name.c_str());

		root = CastLogicalOperatorToTypes(root_select.types, insert->expected_types, std::move(root_select.plan));
	} else {
		root = make_uniq<LogicalDummyScan>(GenerateTableIndex());
	}
	insert->AddChild(std::move(root));

	BindOnConflictClause(*insert, table, stmt);

	if (!stmt.returning_list.empty()) {
		insert->return_chunk = true;
		result.types.clear();
		result.names.clear();
		auto insert_table_index = GenerateTableIndex();
		insert->table_index = insert_table_index;
		unique_ptr<LogicalOperator> index_as_logicaloperator = std::move(insert);

		return BindReturning(std::move(stmt.returning_list), table, stmt.table_ref ? stmt.table_ref->alias : string(),
		                     insert_table_index, std::move(index_as_logicaloperator), std::move(result));
	}

	D_ASSERT(result.types.size() == result.names.size());
	result.plan = std::move(insert);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb



#include <algorithm>

namespace duckdb {

BoundStatement Binder::Bind(LoadStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_LOAD, std::move(stmt.info));
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb


#include <algorithm>

namespace duckdb {

idx_t GetMaxTableIndex(LogicalOperator &op) {
	idx_t result = 0;
	for (auto &child : op.children) {
		auto max_child_index = GetMaxTableIndex(*child);
		result = MaxValue<idx_t>(result, max_child_index);
	}
	auto indexes = op.GetTableIndex();
	for (auto &index : indexes) {
		result = MaxValue<idx_t>(result, index);
	}
	return result;
}

BoundStatement Binder::Bind(LogicalPlanStatement &stmt) {
	BoundStatement result;
	result.types = stmt.plan->types;
	for (idx_t i = 0; i < result.types.size(); i++) {
		result.names.push_back(StringUtil::Format("col%d", i));
	}
	result.plan = std::move(stmt.plan);
	properties.allow_stream_result = true;
	properties.return_type = StatementReturnType::QUERY_RESULT; // TODO could also be something else

	if (parent) {
		throw InternalException("LogicalPlanStatement should be bound in root binder");
	}
	bound_tables = GetMaxTableIndex(*result.plan) + 1;
	return result;
}

} // namespace duckdb







namespace duckdb {

BoundStatement Binder::Bind(PragmaStatement &stmt) {
	// bind the pragma function
	auto &entry =
	    Catalog::GetEntry<PragmaFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, stmt.info->name);
	string error;
	FunctionBinder function_binder(context);
	idx_t bound_idx = function_binder.BindFunction(entry.name, entry.functions, *stmt.info, error);
	if (bound_idx == DConstants::INVALID_INDEX) {
		throw BinderException(FormatError(stmt.stmt_location, error));
	}
	auto bound_function = entry.functions.GetFunctionByOffset(bound_idx);
	if (!bound_function.function) {
		throw BinderException("PRAGMA function does not have a function specified");
	}

	// bind and check named params
	QueryErrorContext error_context(root_statement, stmt.stmt_location);
	BindNamedParameters(bound_function.named_parameters, stmt.info->named_parameters, error_context,
	                    bound_function.name);

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalPragma>(bound_function, *stmt.info);
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb





namespace duckdb {

BoundStatement Binder::Bind(PrepareStatement &stmt) {
	Planner prepared_planner(context);
	auto prepared_data = prepared_planner.PrepareSQLStatement(std::move(stmt.statement));
	this->bound_tables = prepared_planner.binder->bound_tables;

	auto prepare = make_uniq<LogicalPrepare>(stmt.name, std::move(prepared_data), std::move(prepared_planner.plan));
	// we can always prepare, even if the transaction has been invalidated
	// this is required because most clients ALWAYS invoke prepared statements
	properties.requires_valid_transaction = false;
	properties.allow_stream_result = false;
	properties.bound_all_parameters = true;
	properties.parameter_count = 0;
	properties.return_type = StatementReturnType::NOTHING;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = std::move(prepare);
	return result;
}

} // namespace duckdb







namespace duckdb {

BoundStatement Binder::Bind(RelationStatement &stmt) {
	return stmt.relation->Bind(*this);
}

} // namespace duckdb




namespace duckdb {

BoundStatement Binder::Bind(SelectStatement &stmt) {
	properties.allow_stream_result = true;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return Bind(*stmt.node);
}

} // namespace duckdb




#include <algorithm>

namespace duckdb {

BoundStatement Binder::Bind(SetVariableStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalSet>(stmt.name, stmt.value, stmt.scope);
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(ResetVariableStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	result.plan = make_uniq<LogicalReset>(stmt.name, stmt.scope);
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(SetStatement &stmt) {
	switch (stmt.set_type) {
	case SetType::SET: {
		auto &set_stmt = stmt.Cast<SetVariableStatement>();
		return Bind(set_stmt);
	}
	case SetType::RESET: {
		auto &set_stmt = stmt.Cast<ResetVariableStatement>();
		return Bind(set_stmt);
	}
	default:
		throw NotImplementedException("Type not implemented for SetType");
	}
}

} // namespace duckdb




namespace duckdb {

BoundStatement Binder::Bind(ShowStatement &stmt) {
	BoundStatement result;

	if (stmt.info->is_summary) {
		return BindSummarize(stmt);
	}
	auto plan = Bind(*stmt.info->query);
	stmt.info->types = plan.types;
	stmt.info->aliases = plan.names;

	auto show = make_uniq<LogicalShow>(std::move(plan.plan));
	show->types_select = plan.types;
	show->aliases = plan.names;

	result.plan = std::move(show);

	result.names = {"column_name", "column_type", "null", "key", "default", "extra"};
	result.types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb









//! This file contains the binder definitions for statements that do not need to be bound at all and only require a
//! straightforward conversion

namespace duckdb {

BoundStatement Binder::Bind(AlterStatement &stmt) {
	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
	auto entry = Catalog::GetEntry(context, stmt.info->GetCatalogType(), stmt.info->catalog, stmt.info->schema,
	                               stmt.info->name, stmt.info->if_not_found);
	if (entry) {
		auto &catalog = entry->ParentCatalog();
		if (!entry->temporary) {
			// we can only alter temporary tables/views in read-only mode
			properties.modified_databases.insert(catalog.GetName());
		}
		stmt.info->catalog = catalog.GetName();
		stmt.info->schema = entry->ParentSchema().name;
	}
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_ALTER, std::move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

BoundStatement Binder::Bind(TransactionStatement &stmt) {
	// transaction statements do not require a valid transaction
	properties.requires_valid_transaction = stmt.info->type == TransactionType::BEGIN_TRANSACTION;

	BoundStatement result;
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_TRANSACTION, std::move(stmt.info));
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb









namespace duckdb {

static unique_ptr<ParsedExpression> SummarizeWrapUnnest(vector<unique_ptr<ParsedExpression>> &children,
                                                        const string &alias) {
	auto list_function = make_uniq<FunctionExpression>("list_value", std::move(children));
	vector<unique_ptr<ParsedExpression>> unnest_children;
	unnest_children.push_back(std::move(list_function));
	auto unnest_function = make_uniq<FunctionExpression>("unnest", std::move(unnest_children));
	unnest_function->alias = alias;
	return std::move(unnest_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateAggregate(const string &aggregate, string column_name) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>(std::move(column_name)));
	auto aggregate_function = make_uniq<FunctionExpression>(aggregate, std::move(children));
	auto cast_function = make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(aggregate_function));
	return std::move(cast_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateAggregate(const string &aggregate, string column_name,
                                                             const Value &modifier) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ColumnRefExpression>(std::move(column_name)));
	children.push_back(make_uniq<ConstantExpression>(modifier));
	auto aggregate_function = make_uniq<FunctionExpression>(aggregate, std::move(children));
	auto cast_function = make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(aggregate_function));
	return std::move(cast_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateCountStar() {
	vector<unique_ptr<ParsedExpression>> children;
	auto aggregate_function = make_uniq<FunctionExpression>("count_star", std::move(children));
	return std::move(aggregate_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateBinaryFunction(const string &op, unique_ptr<ParsedExpression> left,
                                                                  unique_ptr<ParsedExpression> right) {
	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(std::move(left));
	children.push_back(std::move(right));
	auto binary_function = make_uniq<FunctionExpression>(op, std::move(children));
	return std::move(binary_function);
}

static unique_ptr<ParsedExpression> SummarizeCreateNullPercentage(string column_name) {
	auto count_star = make_uniq<CastExpression>(LogicalType::DOUBLE, SummarizeCreateCountStar());
	auto count =
	    make_uniq<CastExpression>(LogicalType::DOUBLE, SummarizeCreateAggregate("count", std::move(column_name)));
	auto null_percentage = SummarizeCreateBinaryFunction("/", std::move(count), std::move(count_star));
	auto negate_x =
	    SummarizeCreateBinaryFunction("-", make_uniq<ConstantExpression>(Value::DOUBLE(1)), std::move(null_percentage));
	auto percentage_x =
	    SummarizeCreateBinaryFunction("*", std::move(negate_x), make_uniq<ConstantExpression>(Value::DOUBLE(100)));
	auto round_x = SummarizeCreateBinaryFunction("round", std::move(percentage_x),
	                                             make_uniq<ConstantExpression>(Value::INTEGER(2)));
	auto concat_x =
	    SummarizeCreateBinaryFunction("concat", std::move(round_x), make_uniq<ConstantExpression>(Value("%")));

	return concat_x;
}

BoundStatement Binder::BindSummarize(ShowStatement &stmt) {
	auto query_copy = stmt.info->query->Copy();

	// we bind the plan once in a child-node to figure out the column names and column types
	auto child_binder = Binder::CreateBinder(context);
	auto plan = child_binder->Bind(*stmt.info->query);
	D_ASSERT(plan.types.size() == plan.names.size());
	vector<unique_ptr<ParsedExpression>> name_children;
	vector<unique_ptr<ParsedExpression>> type_children;
	vector<unique_ptr<ParsedExpression>> min_children;
	vector<unique_ptr<ParsedExpression>> max_children;
	vector<unique_ptr<ParsedExpression>> unique_children;
	vector<unique_ptr<ParsedExpression>> avg_children;
	vector<unique_ptr<ParsedExpression>> std_children;
	vector<unique_ptr<ParsedExpression>> q25_children;
	vector<unique_ptr<ParsedExpression>> q50_children;
	vector<unique_ptr<ParsedExpression>> q75_children;
	vector<unique_ptr<ParsedExpression>> count_children;
	vector<unique_ptr<ParsedExpression>> null_percentage_children;
	auto select = make_uniq<SelectStatement>();
	select->node = std::move(query_copy);
	for (idx_t i = 0; i < plan.names.size(); i++) {
		name_children.push_back(make_uniq<ConstantExpression>(Value(plan.names[i])));
		type_children.push_back(make_uniq<ConstantExpression>(Value(plan.types[i].ToString())));
		min_children.push_back(SummarizeCreateAggregate("min", plan.names[i]));
		max_children.push_back(SummarizeCreateAggregate("max", plan.names[i]));
		unique_children.push_back(SummarizeCreateAggregate("approx_count_distinct", plan.names[i]));
		if (plan.types[i].IsNumeric()) {
			avg_children.push_back(SummarizeCreateAggregate("avg", plan.names[i]));
			std_children.push_back(SummarizeCreateAggregate("stddev", plan.names[i]));
			q25_children.push_back(SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.25)));
			q50_children.push_back(SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.50)));
			q75_children.push_back(SummarizeCreateAggregate("approx_quantile", plan.names[i], Value::FLOAT(0.75)));
		} else {
			avg_children.push_back(make_uniq<ConstantExpression>(Value()));
			std_children.push_back(make_uniq<ConstantExpression>(Value()));
			q25_children.push_back(make_uniq<ConstantExpression>(Value()));
			q50_children.push_back(make_uniq<ConstantExpression>(Value()));
			q75_children.push_back(make_uniq<ConstantExpression>(Value()));
		}
		count_children.push_back(SummarizeCreateCountStar());
		null_percentage_children.push_back(SummarizeCreateNullPercentage(plan.names[i]));
	}
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(select), "summarize_tbl");
	subquery_ref->column_name_alias = plan.names;

	auto select_node = make_uniq<SelectNode>();
	select_node->select_list.push_back(SummarizeWrapUnnest(name_children, "column_name"));
	select_node->select_list.push_back(SummarizeWrapUnnest(type_children, "column_type"));
	select_node->select_list.push_back(SummarizeWrapUnnest(min_children, "min"));
	select_node->select_list.push_back(SummarizeWrapUnnest(max_children, "max"));
	select_node->select_list.push_back(SummarizeWrapUnnest(unique_children, "approx_unique"));
	select_node->select_list.push_back(SummarizeWrapUnnest(avg_children, "avg"));
	select_node->select_list.push_back(SummarizeWrapUnnest(std_children, "std"));
	select_node->select_list.push_back(SummarizeWrapUnnest(q25_children, "q25"));
	select_node->select_list.push_back(SummarizeWrapUnnest(q50_children, "q50"));
	select_node->select_list.push_back(SummarizeWrapUnnest(q75_children, "q75"));
	select_node->select_list.push_back(SummarizeWrapUnnest(count_children, "count"));
	select_node->select_list.push_back(SummarizeWrapUnnest(null_percentage_children, "null_percentage"));
	select_node->from_table = std::move(subquery_ref);

	properties.return_type = StatementReturnType::QUERY_RESULT;
	return Bind(*select_node);
}

} // namespace duckdb


















#include <algorithm>

namespace duckdb {

// This creates a LogicalProjection and moves 'root' into it as a child
// unless there are no expressions to project, in which case it just returns 'root'
unique_ptr<LogicalOperator> Binder::BindUpdateSet(LogicalOperator &op, unique_ptr<LogicalOperator> root,
                                                  UpdateSetInfo &set_info, TableCatalogEntry &table,
                                                  vector<PhysicalIndex> &columns) {
	auto proj_index = GenerateTableIndex();

	vector<unique_ptr<Expression>> projection_expressions;
	D_ASSERT(set_info.columns.size() == set_info.expressions.size());
	for (idx_t i = 0; i < set_info.columns.size(); i++) {
		auto &colname = set_info.columns[i];
		auto &expr = set_info.expressions[i];
		if (!table.ColumnExists(colname)) {
			throw BinderException("Referenced update column %s not found in table!", colname);
		}
		auto &column = table.GetColumn(colname);
		if (column.Generated()) {
			throw BinderException("Cant update column \"%s\" because it is a generated column!", column.Name());
		}
		if (std::find(columns.begin(), columns.end(), column.Physical()) != columns.end()) {
			throw BinderException("Multiple assignments to same column \"%s\"", colname);
		}
		columns.push_back(column.Physical());
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			op.expressions.push_back(make_uniq<BoundDefaultExpression>(column.Type()));
		} else {
			UpdateBinder binder(*this, context);
			binder.target_type = column.Type();
			auto bound_expr = binder.Bind(expr);
			PlanSubqueries(bound_expr, root);

			op.expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    bound_expr->return_type, ColumnBinding(proj_index, projection_expressions.size())));
			projection_expressions.push_back(std::move(bound_expr));
		}
	}
	if (op.type != LogicalOperatorType::LOGICAL_UPDATE && projection_expressions.empty()) {
		return root;
	}
	// now create the projection
	auto proj = make_uniq<LogicalProjection>(proj_index, std::move(projection_expressions));
	proj->AddChild(std::move(root));
	return unique_ptr_cast<LogicalProjection, LogicalOperator>(std::move(proj));
}

BoundStatement Binder::Bind(UpdateStatement &stmt) {
	BoundStatement result;
	unique_ptr<LogicalOperator> root;

	// visit the table reference
	auto bound_table = Bind(*stmt.table);
	if (bound_table->type != TableReferenceType::BASE_TABLE) {
		throw BinderException("Can only update base table!");
	}
	auto &table_binding = bound_table->Cast<BoundBaseTableRef>();
	auto &table = table_binding.table;

	// Add CTEs as bindable
	AddCTEMap(stmt.cte_map);

	optional_ptr<LogicalGet> get;
	if (stmt.from_table) {
		auto from_binder = Binder::CreateBinder(context, this);
		BoundJoinRef bound_crossproduct(JoinRefType::CROSS);
		bound_crossproduct.left = std::move(bound_table);
		bound_crossproduct.right = from_binder->Bind(*stmt.from_table);
		root = CreatePlan(bound_crossproduct);
		get = &root->children[0]->Cast<LogicalGet>();
		bind_context.AddContext(std::move(from_binder->bind_context));
	} else {
		root = CreatePlan(*bound_table);
		get = &root->Cast<LogicalGet>();
	}

	if (!table.temporary) {
		// update of persistent table: not read only!
		properties.modified_databases.insert(table.catalog.GetName());
	}
	auto update = make_uniq<LogicalUpdate>(table);

	// set return_chunk boolean early because it needs uses update_is_del_and_insert logic
	if (!stmt.returning_list.empty()) {
		update->return_chunk = true;
	}
	// bind the default values
	BindDefaultValues(table.GetColumns(), update->bound_defaults);

	// project any additional columns required for the condition/expressions
	if (stmt.set_info->condition) {
		WhereBinder binder(*this, context);
		auto condition = binder.Bind(stmt.set_info->condition);

		PlanSubqueries(condition, root);
		auto filter = make_uniq<LogicalFilter>(std::move(condition));
		filter->AddChild(std::move(root));
		root = std::move(filter);
	}

	D_ASSERT(stmt.set_info);
	D_ASSERT(stmt.set_info->columns.size() == stmt.set_info->expressions.size());

	auto proj_tmp = BindUpdateSet(*update, std::move(root), *stmt.set_info, table, update->columns);
	D_ASSERT(proj_tmp->type == LogicalOperatorType::LOGICAL_PROJECTION);
	auto proj = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(proj_tmp));

	// bind any extra columns necessary for CHECK constraints or indexes
	table.BindUpdateConstraints(*get, *proj, *update, context);

	// finally add the row id column to the projection list
	proj->expressions.push_back(make_uniq<BoundColumnRefExpression>(
	    LogicalType::ROW_TYPE, ColumnBinding(get->table_index, get->column_ids.size())));
	get->column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);

	// set the projection as child of the update node and finalize the result
	update->AddChild(std::move(proj));

	auto update_table_index = GenerateTableIndex();
	update->table_index = update_table_index;
	if (!stmt.returning_list.empty()) {
		unique_ptr<LogicalOperator> update_as_logicaloperator = std::move(update);

		return BindReturning(std::move(stmt.returning_list), table, stmt.table->alias, update_table_index,
		                     std::move(update_as_logicaloperator), std::move(result));
	}

	result.names = {"Count"};
	result.types = {LogicalType::BIGINT};
	result.plan = std::move(update);
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::CHANGED_ROWS;
	return result;
}

} // namespace duckdb








namespace duckdb {

BoundStatement Binder::Bind(VacuumStatement &stmt) {
	BoundStatement result;

	unique_ptr<LogicalOperator> root;

	if (stmt.info->has_table) {
		D_ASSERT(!stmt.info->table);
		D_ASSERT(stmt.info->column_id_map.empty());
		auto bound_table = Bind(*stmt.info->ref);
		if (bound_table->type != TableReferenceType::BASE_TABLE) {
			throw InvalidInputException("Can only vacuum/analyze base tables!");
		}
		auto ref = unique_ptr_cast<BoundTableRef, BoundBaseTableRef>(std::move(bound_table));
		auto &table = ref->table;
		stmt.info->table = &table;

		auto &columns = stmt.info->columns;
		vector<unique_ptr<Expression>> select_list;
		if (columns.empty()) {
			// Empty means ALL columns should be vacuumed/analyzed
			auto &get = ref->get->Cast<LogicalGet>();
			columns.insert(columns.end(), get.names.begin(), get.names.end());
		}

		case_insensitive_set_t column_name_set;
		vector<string> non_generated_column_names;
		for (auto &col_name : columns) {
			if (column_name_set.count(col_name) > 0) {
				throw BinderException("Vacuum the same column twice(same name in column name list)");
			}
			column_name_set.insert(col_name);
			if (!table.ColumnExists(col_name)) {
				throw BinderException("Column with name \"%s\" does not exist", col_name);
			}
			auto &col = table.GetColumn(col_name);
			// ignore generated column
			if (col.Generated()) {
				continue;
			}
			non_generated_column_names.push_back(col_name);
			ColumnRefExpression colref(col_name, table.name);
			auto result = bind_context.BindColumn(colref, 0);
			if (result.HasError()) {
				throw BinderException(result.error);
			}
			select_list.push_back(std::move(result.expression));
		}
		stmt.info->columns = std::move(non_generated_column_names);
		if (!select_list.empty()) {
			auto table_scan = CreatePlan(*ref);
			D_ASSERT(table_scan->type == LogicalOperatorType::LOGICAL_GET);

			auto &get = table_scan->Cast<LogicalGet>();

			D_ASSERT(select_list.size() == get.column_ids.size());
			D_ASSERT(stmt.info->columns.size() == get.column_ids.size());
			for (idx_t i = 0; i < get.column_ids.size(); i++) {
				stmt.info->column_id_map[i] =
				    table.GetColumns().LogicalToPhysical(LogicalIndex(get.column_ids[i])).index;
			}

			auto projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(select_list));
			projection->children.push_back(std::move(table_scan));

			root = std::move(projection);
		} else {
			// eg. CREATE TABLE test (x AS (1));
			//     ANALYZE test;
			// Make it not a SINK so it doesn't have to do anything
			stmt.info->has_table = false;
		}
	}
	auto vacuum = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_VACUUM, std::move(stmt.info));
	if (root) {
		vacuum->children.push_back(std::move(root));
	}

	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};
	result.plan = std::move(vacuum);
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb


















namespace duckdb {

static bool TryLoadExtensionForReplacementScan(ClientContext &context, const string &table_name) {
	auto lower_name = StringUtil::Lower(table_name);
	auto &dbconfig = DBConfig::GetConfig(context);

	if (!dbconfig.options.autoload_known_extensions) {
		return false;
	}

	for (const auto &entry : EXTENSION_FILE_POSTFIXES) {
		if (StringUtil::EndsWith(lower_name, entry.name)) {
			ExtensionHelper::AutoLoadExtension(context, entry.extension);
			return true;
		}
	}

	for (const auto &entry : EXTENSION_FILE_CONTAINS) {
		if (StringUtil::Contains(lower_name, entry.name)) {
			ExtensionHelper::AutoLoadExtension(context, entry.extension);
			return true;
		}
	}

	return false;
}

unique_ptr<BoundTableRef> Binder::BindWithReplacementScan(ClientContext &context, const string &table_name,
                                                          BaseTableRef &ref) {
	auto &config = DBConfig::GetConfig(context);
	if (context.config.use_replacement_scans) {
		for (auto &scan : config.replacement_scans) {
			auto replacement_function = scan.function(context, table_name, scan.data.get());
			if (replacement_function) {
				if (!ref.alias.empty()) {
					// user-provided alias overrides the default alias
					replacement_function->alias = ref.alias;
				} else if (replacement_function->alias.empty()) {
					// if the replacement scan itself did not provide an alias we use the table name
					replacement_function->alias = ref.table_name;
				}
				if (replacement_function->type == TableReferenceType::TABLE_FUNCTION) {
					auto &table_function = replacement_function->Cast<TableFunctionRef>();
					table_function.column_name_alias = ref.column_name_alias;
				} else if (replacement_function->type == TableReferenceType::SUBQUERY) {
					auto &subquery = replacement_function->Cast<SubqueryRef>();
					subquery.column_name_alias = ref.column_name_alias;
				} else {
					throw InternalException("Replacement scan should return either a table function or a subquery");
				}
				return Bind(*replacement_function);
			}
		}
	}

	return nullptr;
}

unique_ptr<BoundTableRef> Binder::Bind(BaseTableRef &ref) {
	QueryErrorContext error_context(root_statement, ref.query_location);
	// CTEs and views are also referred to using BaseTableRefs, hence need to distinguish here
	// check if the table name refers to a CTE

	// CTE name should never be qualified (i.e. schema_name should be empty)
	optional_ptr<CommonTableExpressionInfo> found_cte = nullptr;
	if (ref.schema_name.empty()) {
		found_cte = FindCTE(ref.table_name, ref.table_name == alias);
	}

	if (found_cte) {
		// Check if there is a CTE binding in the BindContext
		auto &cte = *found_cte;
		auto ctebinding = bind_context.GetCTEBinding(ref.table_name);
		if (!ctebinding) {
			if (CTEIsAlreadyBound(cte)) {
				throw BinderException(
				    "Circular reference to CTE \"%s\", There are two possible solutions. \n1. use WITH RECURSIVE to "
				    "use recursive CTEs. \n2. If "
				    "you want to use the TABLE name \"%s\" the same as the CTE name, please explicitly add "
				    "\"SCHEMA\" before table name. You can try \"main.%s\" (main is the duckdb default schema)",
				    ref.table_name, ref.table_name, ref.table_name);
			}
			// Move CTE to subquery and bind recursively
			SubqueryRef subquery(unique_ptr_cast<SQLStatement, SelectStatement>(cte.query->Copy()));
			subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
			subquery.column_name_alias = cte.aliases;
			for (idx_t i = 0; i < ref.column_name_alias.size(); i++) {
				if (i < subquery.column_name_alias.size()) {
					subquery.column_name_alias[i] = ref.column_name_alias[i];
				} else {
					subquery.column_name_alias.push_back(ref.column_name_alias[i]);
				}
			}
			return Bind(subquery, found_cte);
		} else {
			// There is a CTE binding in the BindContext.
			// This can only be the case if there is a recursive CTE,
			// or a materialized CTE present.
			auto index = GenerateTableIndex();
			auto materialized = cte.materialized;
			if (materialized == CTEMaterialize::CTE_MATERIALIZE_DEFAULT) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
				materialized = CTEMaterialize::CTE_MATERIALIZE_ALWAYS;
#else
				materialized = CTEMaterialize::CTE_MATERIALIZE_NEVER;
#endif
			}
			auto result = make_uniq<BoundCTERef>(index, ctebinding->index, materialized);
			auto b = ctebinding;
			auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
			auto names = BindContext::AliasColumnNames(alias, b->names, ref.column_name_alias);

			bind_context.AddGenericBinding(index, alias, names, b->types);
			// Update references to CTE
			auto cteref = bind_context.cte_references[ref.table_name];
			(*cteref)++;

			result->types = b->types;
			result->bound_columns = std::move(names);
			return std::move(result);
		}
	}
	// not a CTE
	// extract a table or view from the catalog
	BindSchemaOrCatalog(ref.catalog_name, ref.schema_name);
	auto table_or_view = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, ref.catalog_name, ref.schema_name,
	                                       ref.table_name, OnEntryNotFound::RETURN_NULL, error_context);
	// we still didn't find the table
	if (GetBindingMode() == BindingMode::EXTRACT_NAMES) {
		if (!table_or_view || table_or_view->type == CatalogType::TABLE_ENTRY) {
			// if we are in EXTRACT_NAMES, we create a dummy table ref
			AddTableName(ref.table_name);

			// add a bind context entry
			auto table_index = GenerateTableIndex();
			auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
			vector<LogicalType> types {LogicalType::INTEGER};
			vector<string> names {"__dummy_col" + to_string(table_index)};
			bind_context.AddGenericBinding(table_index, alias, names, types);
			return make_uniq_base<BoundTableRef, BoundEmptyTableRef>(table_index);
		}
	}
	if (!table_or_view) {
		string table_name = ref.catalog_name;
		if (!ref.schema_name.empty()) {
			table_name += (!table_name.empty() ? "." : "") + ref.schema_name;
		}
		table_name += (!table_name.empty() ? "." : "") + ref.table_name;
		// table could not be found: try to bind a replacement scan
		// Try replacement scan bind
		auto replacement_scan_bind_result = BindWithReplacementScan(context, table_name, ref);
		if (replacement_scan_bind_result) {
			return replacement_scan_bind_result;
		}

		// Try autoloading an extension, then retry the replacement scan bind
		auto extension_loaded = TryLoadExtensionForReplacementScan(context, table_name);
		if (extension_loaded) {
			replacement_scan_bind_result = BindWithReplacementScan(context, table_name, ref);
			if (replacement_scan_bind_result) {
				return replacement_scan_bind_result;
			}
		}

		// could not find an alternative: bind again to get the error
		table_or_view = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, ref.catalog_name, ref.schema_name,
		                                  ref.table_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
	}
	switch (table_or_view->type) {
	case CatalogType::TABLE_ENTRY: {
		// base table: create the BoundBaseTableRef node
		auto table_index = GenerateTableIndex();
		auto &table = table_or_view->Cast<TableCatalogEntry>();

		unique_ptr<FunctionData> bind_data;
		auto scan_function = table.GetScanFunction(context, bind_data);
		auto alias = ref.alias.empty() ? ref.table_name : ref.alias;
		// TODO: bundle the type and name vector in a struct (e.g PackedColumnMetadata)
		vector<LogicalType> table_types;
		vector<string> table_names;
		vector<TableColumnType> table_categories;

		vector<LogicalType> return_types;
		vector<string> return_names;
		for (auto &col : table.GetColumns().Logical()) {
			table_types.push_back(col.Type());
			table_names.push_back(col.Name());
			return_types.push_back(col.Type());
			return_names.push_back(col.Name());
		}
		table_names = BindContext::AliasColumnNames(alias, table_names, ref.column_name_alias);

		auto logical_get = make_uniq<LogicalGet>(table_index, scan_function, std::move(bind_data),
		                                         std::move(return_types), std::move(return_names));
		bind_context.AddBaseTable(table_index, alias, table_names, table_types, logical_get->column_ids,
		                          logical_get->GetTable().get());
		return make_uniq_base<BoundTableRef, BoundBaseTableRef>(table, std::move(logical_get));
	}
	case CatalogType::VIEW_ENTRY: {
		// the node is a view: get the query that the view represents
		auto &view_catalog_entry = table_or_view->Cast<ViewCatalogEntry>();
		// We need to use a new binder for the view that doesn't reference any CTEs
		// defined for this binder so there are no collisions between the CTEs defined
		// for the view and for the current query
		bool inherit_ctes = false;
		auto view_binder = Binder::CreateBinder(context, this, inherit_ctes);
		view_binder->can_contain_nulls = true;
		SubqueryRef subquery(unique_ptr_cast<SQLStatement, SelectStatement>(view_catalog_entry.query->Copy()));
		subquery.alias = ref.alias.empty() ? ref.table_name : ref.alias;
		subquery.column_name_alias =
		    BindContext::AliasColumnNames(subquery.alias, view_catalog_entry.aliases, ref.column_name_alias);
		// bind the child subquery
		view_binder->AddBoundView(view_catalog_entry);
		auto bound_child = view_binder->Bind(subquery);
		if (!view_binder->correlated_columns.empty()) {
			throw BinderException("Contents of view were altered - view bound correlated columns");
		}

		D_ASSERT(bound_child->type == TableReferenceType::SUBQUERY);
		// verify that the types and names match up with the expected types and names
		auto &bound_subquery = bound_child->Cast<BoundSubqueryRef>();
		if (GetBindingMode() != BindingMode::EXTRACT_NAMES &&
		    bound_subquery.subquery->types != view_catalog_entry.types) {
			throw BinderException("Contents of view were altered: types don't match!");
		}
		bind_context.AddView(bound_subquery.subquery->GetRootIndex(), subquery.alias, subquery,
		                     *bound_subquery.subquery, &view_catalog_entry);
		return bound_child;
	}
	default:
		throw InternalException("Catalog entry type");
	}
}
} // namespace duckdb




namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(EmptyTableRef &ref) {
	return make_uniq<BoundEmptyTableRef>(GenerateTableIndex());
}

} // namespace duckdb







namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(ExpressionListRef &expr) {
	auto result = make_uniq<BoundExpressionListRef>();
	result->types = expr.expected_types;
	result->names = expr.expected_names;
	// bind value list
	InsertBinder binder(*this, context);
	binder.target_type = LogicalType(LogicalTypeId::INVALID);
	for (idx_t list_idx = 0; list_idx < expr.values.size(); list_idx++) {
		auto &expression_list = expr.values[list_idx];
		if (result->names.empty()) {
			// no names provided, generate them
			for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
				result->names.push_back("col" + to_string(val_idx));
			}
		}

		vector<unique_ptr<Expression>> list;
		for (idx_t val_idx = 0; val_idx < expression_list.size(); val_idx++) {
			if (!result->types.empty()) {
				D_ASSERT(result->types.size() == expression_list.size());
				binder.target_type = result->types[val_idx];
			}
			auto expr = binder.Bind(expression_list[val_idx]);
			list.push_back(std::move(expr));
		}
		result->values.push_back(std::move(list));
	}
	if (result->types.empty() && !expr.values.empty()) {
		// there are no types specified
		// we have to figure out the result types
		// for each column, we iterate over all of the expressions and select the max logical type
		// we initialize all types to SQLNULL
		result->types.resize(expr.values[0].size(), LogicalType::SQLNULL);
		// now loop over the lists and select the max logical type
		for (idx_t list_idx = 0; list_idx < result->values.size(); list_idx++) {
			auto &list = result->values[list_idx];
			for (idx_t val_idx = 0; val_idx < list.size(); val_idx++) {
				result->types[val_idx] =
				    LogicalType::MaxLogicalType(result->types[val_idx], list[val_idx]->return_type);
			}
		}
		// finally do another loop over the expressions and add casts where required
		for (idx_t list_idx = 0; list_idx < result->values.size(); list_idx++) {
			auto &list = result->values[list_idx];
			for (idx_t val_idx = 0; val_idx < list.size(); val_idx++) {
				list[val_idx] =
				    BoundCastExpression::AddCastToType(context, std::move(list[val_idx]), result->types[val_idx]);
			}
		}
	}
	result->bind_index = GenerateTableIndex();
	bind_context.AddGenericBinding(result->bind_index, expr.alias, result->names, result->types);
	return std::move(result);
}

} // namespace duckdb















namespace duckdb {

static unique_ptr<ParsedExpression> BindColumn(Binder &binder, ClientContext &context, const string &alias,
                                               const string &column_name) {
	auto expr = make_uniq_base<ParsedExpression, ColumnRefExpression>(column_name, alias);
	ExpressionBinder expr_binder(binder, context);
	auto result = expr_binder.Bind(expr);
	return make_uniq<BoundExpression>(std::move(result));
}

static unique_ptr<ParsedExpression> AddCondition(ClientContext &context, Binder &left_binder, Binder &right_binder,
                                                 const string &left_alias, const string &right_alias,
                                                 const string &column_name, ExpressionType type) {
	ExpressionBinder expr_binder(left_binder, context);
	auto left = BindColumn(left_binder, context, left_alias, column_name);
	auto right = BindColumn(right_binder, context, right_alias, column_name);
	return make_uniq<ComparisonExpression>(type, std::move(left), std::move(right));
}

bool Binder::TryFindBinding(const string &using_column, const string &join_side, string &result) {
	// for each using column, get the matching binding
	auto bindings = bind_context.GetMatchingBindings(using_column);
	if (bindings.empty()) {
		return false;
	}
	// find the join binding
	for (auto &binding : bindings) {
		if (!result.empty()) {
			string error = "Column name \"";
			error += using_column;
			error += "\" is ambiguous: it exists more than once on ";
			error += join_side;
			error += " side of join.\nCandidates:";
			for (auto &binding : bindings) {
				error += "\n\t";
				error += binding;
				error += ".";
				error += bind_context.GetActualColumnName(binding, using_column);
			}
			throw BinderException(error);
		} else {
			result = binding;
		}
	}
	return true;
}

string Binder::FindBinding(const string &using_column, const string &join_side) {
	string result;
	if (!TryFindBinding(using_column, join_side, result)) {
		throw BinderException("Column \"%s\" does not exist on %s side of join!", using_column, join_side);
	}
	return result;
}

static void AddUsingBindings(UsingColumnSet &set, optional_ptr<UsingColumnSet> input_set, const string &input_binding) {
	if (input_set) {
		for (auto &entry : input_set->bindings) {
			set.bindings.insert(entry);
		}
	} else {
		set.bindings.insert(input_binding);
	}
}

static void SetPrimaryBinding(UsingColumnSet &set, JoinType join_type, const string &left_binding,
                              const string &right_binding) {
	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::INNER:
	case JoinType::SEMI:
	case JoinType::ANTI:
		set.primary_binding = left_binding;
		break;
	case JoinType::RIGHT:
		set.primary_binding = right_binding;
		break;
	default:
		break;
	}
}

string Binder::RetrieveUsingBinding(Binder &current_binder, optional_ptr<UsingColumnSet> current_set,
                                    const string &using_column, const string &join_side) {
	string binding;
	if (!current_set) {
		binding = current_binder.FindBinding(using_column, join_side);
	} else {
		binding = current_set->primary_binding;
	}
	return binding;
}

static vector<string> RemoveDuplicateUsingColumns(const vector<string> &using_columns) {
	vector<string> result;
	case_insensitive_set_t handled_columns;
	for (auto &using_column : using_columns) {
		if (handled_columns.find(using_column) == handled_columns.end()) {
			handled_columns.insert(using_column);
			result.push_back(using_column);
		}
	}
	return result;
}

unique_ptr<BoundTableRef> Binder::Bind(JoinRef &ref) {
	auto result = make_uniq<BoundJoinRef>(ref.ref_type);
	result->left_binder = Binder::CreateBinder(context, this);
	result->right_binder = Binder::CreateBinder(context, this);
	auto &left_binder = *result->left_binder;
	auto &right_binder = *result->right_binder;

	result->type = ref.type;
	result->left = left_binder.Bind(*ref.left);
	{
		LateralBinder binder(left_binder, context);
		result->right = right_binder.Bind(*ref.right);
		bool is_lateral = false;
		// Store the correlated columns in the right binder in bound ref for planning of LATERALs
		// Ignore the correlated columns in the left binder, flattening handles those correlations
		result->correlated_columns = right_binder.correlated_columns;
		// Find correlations for the current join
		for (auto &cor_col : result->correlated_columns) {
			if (cor_col.depth == 1) {
				// Depth 1 indicates columns binding from the left indicating a lateral join
				is_lateral = true;
				break;
			}
		}
		result->lateral = is_lateral;
		if (result->lateral) {
			// lateral join: can only be an INNER or LEFT join
			if (ref.type != JoinType::INNER && ref.type != JoinType::LEFT) {
				throw BinderException("The combining JOIN type must be INNER or LEFT for a LATERAL reference");
			}
		}
	}

	vector<unique_ptr<ParsedExpression>> extra_conditions;
	vector<string> extra_using_columns;
	switch (ref.ref_type) {
	case JoinRefType::NATURAL: {
		// natural join, figure out which column names are present in both sides of the join
		// first bind the left hand side and get a list of all the tables and column names
		case_insensitive_set_t lhs_columns;
		auto &lhs_binding_list = left_binder.bind_context.GetBindingsList();
		for (auto &binding : lhs_binding_list) {
			for (auto &column_name : binding.get().names) {
				lhs_columns.insert(column_name);
			}
		}
		// now bind the rhs
		for (auto &column_name : lhs_columns) {
			auto right_using_binding = right_binder.bind_context.GetUsingBinding(column_name);

			string right_binding;
			// loop over the set of lhs columns, and figure out if there is a table in the rhs with the same name
			if (!right_using_binding) {
				if (!right_binder.TryFindBinding(column_name, "right", right_binding)) {
					// no match found for this column on the rhs: skip
					continue;
				}
			}
			extra_using_columns.push_back(column_name);
		}
		if (extra_using_columns.empty()) {
			// no matching bindings found in natural join: throw an exception
			string error_msg = "No columns found to join on in NATURAL JOIN.\n";
			error_msg += "Use CROSS JOIN if you intended for this to be a cross-product.";
			// gather all left/right candidates
			string left_candidates, right_candidates;
			auto &rhs_binding_list = right_binder.bind_context.GetBindingsList();
			for (auto &binding_ref : lhs_binding_list) {
				auto &binding = binding_ref.get();
				for (auto &column_name : binding.names) {
					if (!left_candidates.empty()) {
						left_candidates += ", ";
					}
					left_candidates += binding.alias + "." + column_name;
				}
			}
			for (auto &binding_ref : rhs_binding_list) {
				auto &binding = binding_ref.get();
				for (auto &column_name : binding.names) {
					if (!right_candidates.empty()) {
						right_candidates += ", ";
					}
					right_candidates += binding.alias + "." + column_name;
				}
			}
			error_msg += "\n   Left candidates: " + left_candidates;
			error_msg += "\n   Right candidates: " + right_candidates;
			throw BinderException(FormatError(ref, error_msg));
		}
		break;
	}
	case JoinRefType::REGULAR:
	case JoinRefType::ASOF:
		if (!ref.using_columns.empty()) {
			// USING columns
			D_ASSERT(!result->condition);
			extra_using_columns = ref.using_columns;
		}
		break;

	case JoinRefType::CROSS:
	case JoinRefType::POSITIONAL:
	case JoinRefType::DEPENDENT:
		break;
	}
	extra_using_columns = RemoveDuplicateUsingColumns(extra_using_columns);

	if (!extra_using_columns.empty()) {
		vector<optional_ptr<UsingColumnSet>> left_using_bindings;
		vector<optional_ptr<UsingColumnSet>> right_using_bindings;
		for (idx_t i = 0; i < extra_using_columns.size(); i++) {
			auto &using_column = extra_using_columns[i];
			// we check if there is ALREADY a using column of the same name in the left and right set
			// this can happen if we chain USING clauses
			// e.g. x JOIN y USING (c) JOIN z USING (c)
			auto left_using_binding = left_binder.bind_context.GetUsingBinding(using_column);
			auto right_using_binding = right_binder.bind_context.GetUsingBinding(using_column);
			if (!left_using_binding) {
				left_binder.bind_context.GetMatchingBinding(using_column);
			}
			if (!right_using_binding) {
				right_binder.bind_context.GetMatchingBinding(using_column);
			}
			left_using_bindings.push_back(left_using_binding);
			right_using_bindings.push_back(right_using_binding);
		}

		for (idx_t i = 0; i < extra_using_columns.size(); i++) {
			auto &using_column = extra_using_columns[i];
			string left_binding;
			string right_binding;

			auto set = make_uniq<UsingColumnSet>();
			auto &left_using_binding = left_using_bindings[i];
			auto &right_using_binding = right_using_bindings[i];
			left_binding = RetrieveUsingBinding(left_binder, left_using_binding, using_column, "left");
			right_binding = RetrieveUsingBinding(right_binder, right_using_binding, using_column, "right");

			// Last column of ASOF JOIN ... USING is >=
			const auto type = (ref.ref_type == JoinRefType::ASOF && i == extra_using_columns.size() - 1)
			                      ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
			                      : ExpressionType::COMPARE_EQUAL;

			extra_conditions.push_back(
			    AddCondition(context, left_binder, right_binder, left_binding, right_binding, using_column, type));

			AddUsingBindings(*set, left_using_binding, left_binding);
			AddUsingBindings(*set, right_using_binding, right_binding);
			SetPrimaryBinding(*set, ref.type, left_binding, right_binding);
			bind_context.TransferUsingBinding(left_binder.bind_context, left_using_binding, *set, left_binding,
			                                  using_column);
			bind_context.TransferUsingBinding(right_binder.bind_context, right_using_binding, *set, right_binding,
			                                  using_column);
			AddUsingBindingSet(std::move(set));
		}
	}

	auto right_bindings_list_copy = right_binder.bind_context.GetBindingsList();

	bind_context.AddContext(std::move(left_binder.bind_context));
	bind_context.AddContext(std::move(right_binder.bind_context));

	// Update the correlated columns for the parent binder
	// For the left binder, depth >= 1 indicates correlations from the parent binder
	for (const auto &col : left_binder.correlated_columns) {
		if (col.depth >= 1) {
			AddCorrelatedColumn(col);
		}
	}
	// For the right binder, depth > 1 indicates correlations from the parent binder
	// (depth = 1 indicates correlations from the left side of the join)
	for (auto col : right_binder.correlated_columns) {
		if (col.depth > 1) {
			// Decrement the depth to account for the effect of the lateral binder
			col.depth--;
			AddCorrelatedColumn(col);
		}
	}

	for (auto &condition : extra_conditions) {
		if (ref.condition) {
			ref.condition = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(ref.condition),
			                                                 std::move(condition));
		} else {
			ref.condition = std::move(condition);
		}
	}
	if (ref.condition) {
		WhereBinder binder(*this, context);
		result->condition = binder.Bind(ref.condition);
	}

	if (result->type == JoinType::SEMI || result->type == JoinType::ANTI) {
		bind_context.RemoveContext(right_bindings_list_copy);
	}

	return std::move(result);
}

} // namespace duckdb


namespace duckdb {

void Binder::BindNamedParameters(named_parameter_type_map_t &types, named_parameter_map_t &values,
                                 QueryErrorContext &error_context, string &func_name) {
	for (auto &kv : values) {
		auto entry = types.find(kv.first);
		if (entry == types.end()) {
			// create a list of named parameters for the error
			string named_params;
			for (auto &kv : types) {
				named_params += "    ";
				named_params += kv.first;
				named_params += " ";
				named_params += kv.second.ToString();
				named_params += "\n";
			}
			string error_msg;
			if (named_params.empty()) {
				error_msg = "Function does not accept any named parameters.";
			} else {
				error_msg = "Candidates:\n" + named_params;
			}
			throw BinderException(error_context.FormatError("Invalid named parameter \"%s\" for function %s\n%s",
			                                                kv.first, func_name, error_msg));
		}
		if (entry->second.id() != LogicalTypeId::ANY) {
			kv.second = kv.second.DefaultCastAs(entry->second);
		}
	}
}

} // namespace duckdb






















namespace duckdb {

static void ConstructPivots(PivotRef &ref, vector<PivotValueElement> &pivot_values, idx_t pivot_idx = 0,
                            const PivotValueElement &current_value = PivotValueElement()) {
	auto &pivot = ref.pivots[pivot_idx];
	bool last_pivot = pivot_idx + 1 == ref.pivots.size();
	for (auto &entry : pivot.entries) {
		PivotValueElement new_value = current_value;
		string name = entry.alias;
		D_ASSERT(entry.values.size() == pivot.pivot_expressions.size());
		for (idx_t v = 0; v < entry.values.size(); v++) {
			auto &value = entry.values[v];
			new_value.values.push_back(value);
			if (entry.alias.empty()) {
				if (name.empty()) {
					name = value.ToString();
				} else {
					name += "_" + value.ToString();
				}
			}
		}
		if (!current_value.name.empty()) {
			new_value.name = current_value.name + "_" + name;
		} else {
			new_value.name = std::move(name);
		}
		if (last_pivot) {
			pivot_values.push_back(std::move(new_value));
		} else {
			// need to recurse
			ConstructPivots(ref, pivot_values, pivot_idx + 1, new_value);
		}
	}
}

static void ExtractPivotExpressions(ParsedExpression &expr, case_insensitive_set_t &handled_columns) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &child_colref = expr.Cast<ColumnRefExpression>();
		if (child_colref.IsQualified()) {
			throw BinderException("PIVOT expression cannot contain qualified columns");
		}
		handled_columns.insert(child_colref.GetColumnName());
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](ParsedExpression &child) { ExtractPivotExpressions(child, handled_columns); });
}

static unique_ptr<SelectNode> ConstructInitialGrouping(PivotRef &ref, vector<unique_ptr<ParsedExpression>> all_columns,
                                                       const case_insensitive_set_t &handled_columns) {
	auto subquery = make_uniq<SelectNode>();
	subquery->from_table = std::move(ref.source);
	if (ref.groups.empty()) {
		// if rows are not specified any columns that are not pivoted/aggregated on are added to the GROUP BY clause
		for (auto &entry : all_columns) {
			if (entry->type != ExpressionType::COLUMN_REF) {
				throw InternalException("Unexpected child of pivot source - not a ColumnRef");
			}
			auto &columnref = entry->Cast<ColumnRefExpression>();
			if (handled_columns.find(columnref.GetColumnName()) == handled_columns.end()) {
				// not handled - add to grouping set
				subquery->groups.group_expressions.push_back(
				    make_uniq<ConstantExpression>(Value::INTEGER(subquery->select_list.size() + 1)));
				subquery->select_list.push_back(make_uniq<ColumnRefExpression>(columnref.GetColumnName()));
			}
		}
	} else {
		// if rows are specified only the columns mentioned in rows are added as groups
		for (auto &row : ref.groups) {
			subquery->groups.group_expressions.push_back(
			    make_uniq<ConstantExpression>(Value::INTEGER(subquery->select_list.size() + 1)));
			subquery->select_list.push_back(make_uniq<ColumnRefExpression>(row));
		}
	}
	return subquery;
}

static unique_ptr<SelectNode> PivotFilteredAggregate(PivotRef &ref, vector<unique_ptr<ParsedExpression>> all_columns,
                                                     const case_insensitive_set_t &handled_columns,
                                                     vector<PivotValueElement> pivot_values) {
	auto subquery = ConstructInitialGrouping(ref, std::move(all_columns), handled_columns);

	// push the filtered aggregates
	for (auto &pivot_value : pivot_values) {
		unique_ptr<ParsedExpression> filter;
		idx_t pivot_value_idx = 0;
		for (auto &pivot_column : ref.pivots) {
			for (auto &pivot_expr : pivot_column.pivot_expressions) {
				auto column_ref = make_uniq<CastExpression>(LogicalType::VARCHAR, pivot_expr->Copy());
				auto constant_value = make_uniq<ConstantExpression>(pivot_value.values[pivot_value_idx++]);
				auto comp_expr = make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
				                                                 std::move(column_ref), std::move(constant_value));
				if (filter) {
					filter = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(filter),
					                                          std::move(comp_expr));
				} else {
					filter = std::move(comp_expr);
				}
			}
		}
		for (auto &aggregate : ref.aggregates) {
			auto copied_aggr = aggregate->Copy();
			auto &aggr = copied_aggr->Cast<FunctionExpression>();
			aggr.filter = filter->Copy();
			auto &aggr_name = aggregate->alias;
			auto name = pivot_value.name;
			if (ref.aggregates.size() > 1 || !aggr_name.empty()) {
				// if there are multiple aggregates specified we add the name of the aggregate as well
				name += "_" + (aggr_name.empty() ? aggregate->GetName() : aggr_name);
			}
			aggr.alias = name;
			subquery->select_list.push_back(std::move(copied_aggr));
		}
	}
	return subquery;
}

struct PivotBindState {
	vector<string> internal_group_names;
	vector<string> group_names;
	vector<string> aggregate_names;
	vector<string> internal_aggregate_names;
};

static unique_ptr<SelectNode> PivotInitialAggregate(PivotBindState &bind_state, PivotRef &ref,
                                                    vector<unique_ptr<ParsedExpression>> all_columns,
                                                    const case_insensitive_set_t &handled_columns) {
	auto subquery_stage1 = ConstructInitialGrouping(ref, std::move(all_columns), handled_columns);

	idx_t group_count = 0;
	for (auto &expr : subquery_stage1->select_list) {
		bind_state.group_names.push_back(expr->GetName());
		if (expr->alias.empty()) {
			expr->alias = "__internal_pivot_group" + std::to_string(++group_count);
		}
		bind_state.internal_group_names.push_back(expr->alias);
	}
	// group by all of the pivot values
	idx_t pivot_count = 0;
	for (auto &pivot_column : ref.pivots) {
		for (auto &pivot_expr : pivot_column.pivot_expressions) {
			if (pivot_expr->alias.empty()) {
				pivot_expr->alias = "__internal_pivot_ref" + std::to_string(++pivot_count);
			}
			auto pivot_alias = pivot_expr->alias;
			subquery_stage1->groups.group_expressions.push_back(
			    make_uniq<ConstantExpression>(Value::INTEGER(subquery_stage1->select_list.size() + 1)));
			subquery_stage1->select_list.push_back(std::move(pivot_expr));
			pivot_expr = make_uniq<ColumnRefExpression>(std::move(pivot_alias));
		}
	}
	idx_t aggregate_count = 0;
	// finally add the aggregates
	for (auto &aggregate : ref.aggregates) {
		auto aggregate_alias = "__internal_pivot_aggregate" + std::to_string(++aggregate_count);
		bind_state.aggregate_names.push_back(aggregate->alias);
		bind_state.internal_aggregate_names.push_back(aggregate_alias);
		aggregate->alias = std::move(aggregate_alias);
		subquery_stage1->select_list.push_back(std::move(aggregate));
	}
	return subquery_stage1;
}

unique_ptr<ParsedExpression> ConstructPivotExpression(unique_ptr<ParsedExpression> pivot_expr) {
	auto cast = make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(pivot_expr));
	vector<unique_ptr<ParsedExpression>> coalesce_children;
	coalesce_children.push_back(std::move(cast));
	coalesce_children.push_back(make_uniq<ConstantExpression>(Value("NULL")));
	auto coalesce = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_COALESCE, std::move(coalesce_children));
	return std::move(coalesce);
}

static unique_ptr<SelectNode> PivotListAggregate(PivotBindState &bind_state, PivotRef &ref,
                                                 unique_ptr<SelectNode> subquery_stage1) {
	auto subquery_stage2 = make_uniq<SelectNode>();
	// wrap the subquery of stage 1
	auto subquery_select = make_uniq<SelectStatement>();
	subquery_select->node = std::move(subquery_stage1);
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(subquery_select));

	// add all of the groups
	for (idx_t gr = 0; gr < bind_state.internal_group_names.size(); gr++) {
		subquery_stage2->groups.group_expressions.push_back(
		    make_uniq<ConstantExpression>(Value::INTEGER(subquery_stage2->select_list.size() + 1)));
		auto group_reference = make_uniq<ColumnRefExpression>(bind_state.internal_group_names[gr]);
		group_reference->alias = bind_state.internal_group_names[gr];
		subquery_stage2->select_list.push_back(std::move(group_reference));
	}

	// construct the list aggregates
	for (idx_t aggr = 0; aggr < bind_state.internal_aggregate_names.size(); aggr++) {
		auto colref = make_uniq<ColumnRefExpression>(bind_state.internal_aggregate_names[aggr]);
		vector<unique_ptr<ParsedExpression>> list_children;
		list_children.push_back(std::move(colref));
		auto aggregate = make_uniq<FunctionExpression>("list", std::move(list_children));
		aggregate->alias = bind_state.internal_aggregate_names[aggr];
		subquery_stage2->select_list.push_back(std::move(aggregate));
	}
	// construct the pivot list
	auto pivot_name = "__internal_pivot_name";
	unique_ptr<ParsedExpression> expr;
	for (auto &pivot : ref.pivots) {
		for (auto &pivot_expr : pivot.pivot_expressions) {
			// coalesce(pivot::VARCHAR, 'NULL')
			auto coalesce = ConstructPivotExpression(std::move(pivot_expr));
			if (!expr) {
				expr = std::move(coalesce);
			} else {
				// string concat
				vector<unique_ptr<ParsedExpression>> concat_children;
				concat_children.push_back(std::move(expr));
				concat_children.push_back(make_uniq<ConstantExpression>(Value("_")));
				concat_children.push_back(std::move(coalesce));
				auto concat = make_uniq<FunctionExpression>("concat", std::move(concat_children));
				expr = std::move(concat);
			}
		}
	}
	// list(coalesce)
	vector<unique_ptr<ParsedExpression>> list_children;
	list_children.push_back(std::move(expr));
	auto aggregate = make_uniq<FunctionExpression>("list", std::move(list_children));

	aggregate->alias = pivot_name;
	subquery_stage2->select_list.push_back(std::move(aggregate));

	subquery_stage2->from_table = std::move(subquery_ref);
	return subquery_stage2;
}

static unique_ptr<SelectNode> PivotFinalOperator(PivotBindState &bind_state, PivotRef &ref,
                                                 unique_ptr<SelectNode> subquery,
                                                 vector<PivotValueElement> pivot_values) {
	auto final_pivot_operator = make_uniq<SelectNode>();
	// wrap the subquery of stage 1
	auto subquery_select = make_uniq<SelectStatement>();
	subquery_select->node = std::move(subquery);
	auto subquery_ref = make_uniq<SubqueryRef>(std::move(subquery_select));

	auto bound_pivot = make_uniq<PivotRef>();
	bound_pivot->bound_pivot_values = std::move(pivot_values);
	bound_pivot->bound_group_names = std::move(bind_state.group_names);
	bound_pivot->bound_aggregate_names = std::move(bind_state.aggregate_names);
	bound_pivot->source = std::move(subquery_ref);

	final_pivot_operator->select_list.push_back(make_uniq<StarExpression>());
	final_pivot_operator->from_table = std::move(bound_pivot);
	return final_pivot_operator;
}

void ExtractPivotAggregates(BoundTableRef &node, vector<unique_ptr<Expression>> &aggregates) {
	if (node.type != TableReferenceType::SUBQUERY) {
		throw InternalException("Pivot - Expected a subquery");
	}
	auto &subq = node.Cast<BoundSubqueryRef>();
	if (subq.subquery->type != QueryNodeType::SELECT_NODE) {
		throw InternalException("Pivot - Expected a select node");
	}
	auto &select = subq.subquery->Cast<BoundSelectNode>();
	if (select.from_table->type != TableReferenceType::SUBQUERY) {
		throw InternalException("Pivot - Expected another subquery");
	}
	auto &subq2 = select.from_table->Cast<BoundSubqueryRef>();
	if (subq2.subquery->type != QueryNodeType::SELECT_NODE) {
		throw InternalException("Pivot - Expected another select node");
	}
	auto &select2 = subq2.subquery->Cast<BoundSelectNode>();
	for (auto &aggr : select2.aggregates) {
		aggregates.push_back(aggr->Copy());
	}
}

unique_ptr<BoundTableRef> Binder::BindBoundPivot(PivotRef &ref) {
	// bind the child table in a child binder
	auto result = make_uniq<BoundPivotRef>();
	result->bind_index = GenerateTableIndex();
	result->child_binder = Binder::CreateBinder(context, this);
	result->child = result->child_binder->Bind(*ref.source);

	auto &aggregates = result->bound_pivot.aggregates;
	ExtractPivotAggregates(*result->child, aggregates);
	if (aggregates.size() != ref.bound_aggregate_names.size()) {
		throw InternalException("Pivot aggregate count mismatch (expected %llu, found %llu)",
		                        ref.bound_aggregate_names.size(), aggregates.size());
	}

	vector<string> child_names;
	vector<LogicalType> child_types;
	result->child_binder->bind_context.GetTypesAndNames(child_names, child_types);

	vector<string> names;
	vector<LogicalType> types;
	// emit the groups
	for (idx_t i = 0; i < ref.bound_group_names.size(); i++) {
		names.push_back(ref.bound_group_names[i]);
		types.push_back(child_types[i]);
	}
	// emit the pivot columns
	for (auto &pivot_value : ref.bound_pivot_values) {
		for (idx_t aggr_idx = 0; aggr_idx < ref.bound_aggregate_names.size(); aggr_idx++) {
			auto &aggr = aggregates[aggr_idx];
			auto &aggr_name = ref.bound_aggregate_names[aggr_idx];
			auto name = pivot_value.name;
			if (aggregates.size() > 1 || !aggr_name.empty()) {
				// if there are multiple aggregates specified we add the name of the aggregate as well
				name += "_" + (aggr_name.empty() ? aggr->GetName() : aggr_name);
			}
			string pivot_str;
			for (auto &value : pivot_value.values) {
				auto str = value.ToString();
				if (pivot_str.empty()) {
					pivot_str = std::move(str);
				} else {
					pivot_str += "_" + str;
				}
			}
			result->bound_pivot.pivot_values.push_back(std::move(pivot_str));
			names.push_back(std::move(name));
			types.push_back(aggr->return_type);
		}
	}
	result->bound_pivot.group_count = ref.bound_group_names.size();
	result->bound_pivot.types = types;
	auto subquery_alias = ref.alias.empty() ? "__unnamed_pivot" : ref.alias;
	bind_context.AddGenericBinding(result->bind_index, subquery_alias, names, types);
	MoveCorrelatedExpressions(*result->child_binder);
	return std::move(result);
}

unique_ptr<SelectNode> Binder::BindPivot(PivotRef &ref, vector<unique_ptr<ParsedExpression>> all_columns) {
	// keep track of the columns by which we pivot/aggregate
	// any columns which are not pivoted/aggregated on are added to the GROUP BY clause
	case_insensitive_set_t handled_columns;
	// parse the aggregate, and extract the referenced columns from the aggregate
	for (auto &aggr : ref.aggregates) {
		if (aggr->type != ExpressionType::FUNCTION) {
			throw BinderException(FormatError(*aggr, "Pivot expression must be an aggregate"));
		}
		if (aggr->HasSubquery()) {
			throw BinderException(FormatError(*aggr, "Pivot expression cannot contain subqueries"));
		}
		if (aggr->IsWindow()) {
			throw BinderException(FormatError(*aggr, "Pivot expression cannot contain window functions"));
		}
		// bind the function as an aggregate to ensure it is an aggregate and not a scalar function
		auto &aggr_function = aggr->Cast<FunctionExpression>();
		(void)Catalog::GetEntry<AggregateFunctionCatalogEntry>(context, aggr_function.catalog, aggr_function.schema,
		                                                       aggr_function.function_name);
		ExtractPivotExpressions(*aggr, handled_columns);
	}

	// first add all pivots to the set of handled columns, and check for duplicates
	idx_t total_pivots = 1;
	for (auto &pivot : ref.pivots) {
		if (!pivot.pivot_enum.empty()) {
			auto type = Catalog::GetType(context, INVALID_CATALOG, INVALID_SCHEMA, pivot.pivot_enum);
			if (type.id() != LogicalTypeId::ENUM) {
				throw BinderException(
				    FormatError(ref, StringUtil::Format("Pivot must reference an ENUM type: \"%s\" is of type \"%s\"",
				                                        pivot.pivot_enum, type.ToString())));
			}
			auto enum_size = EnumType::GetSize(type);
			for (idx_t i = 0; i < enum_size; i++) {
				auto enum_value = EnumType::GetValue(Value::ENUM(i, type));
				PivotColumnEntry entry;
				entry.values.emplace_back(enum_value);
				entry.alias = std::move(enum_value);
				pivot.entries.push_back(std::move(entry));
			}
		}
		total_pivots *= pivot.entries.size();
		// add the pivoted column to the columns that have been handled
		for (auto &pivot_name : pivot.pivot_expressions) {
			ExtractPivotExpressions(*pivot_name, handled_columns);
		}
		value_set_t pivots;
		for (auto &entry : pivot.entries) {
			D_ASSERT(!entry.star_expr);
			Value val;
			if (entry.values.size() == 1) {
				val = entry.values[0];
			} else {
				val = Value::LIST(LogicalType::VARCHAR, entry.values);
			}
			if (pivots.find(val) != pivots.end()) {
				throw BinderException(FormatError(
				    ref, StringUtil::Format("The value \"%s\" was specified multiple times in the IN clause",
				                            val.ToString())));
			}
			if (entry.values.size() != pivot.pivot_expressions.size()) {
				throw ParserException("PIVOT IN list - inconsistent amount of rows - expected %d but got %d",
				                      pivot.pivot_expressions.size(), entry.values.size());
			}
			pivots.insert(val);
		}
	}
	auto &client_config = ClientConfig::GetConfig(context);
	auto pivot_limit = client_config.pivot_limit;
	if (total_pivots >= pivot_limit) {
		throw BinderException("Pivot column limit of %llu exceeded. Use SET pivot_limit=X to increase the limit.",
		                      client_config.pivot_limit);
	}

	// construct the required pivot values recursively
	vector<PivotValueElement> pivot_values;
	ConstructPivots(ref, pivot_values);

	unique_ptr<SelectNode> pivot_node;
	// pivots have three components
	// - the pivots (i.e. future column names)
	// - the groups (i.e. the future row names
	// - the aggregates (i.e. the values of the pivot columns)

	// we have two ways of executing a pivot statement
	// (1) the straightforward manner of filtered aggregates SUM(..) FILTER (pivot_value=X)
	// (2) computing the aggregates once, then using LIST to group the aggregates together with the PIVOT operator
	// -> filtered aggregates are faster when there are FEW pivot values
	// -> LIST is faster when there are MANY pivot values
	// we switch dynamically based on the number of pivots to compute
	if (pivot_values.size() <= client_config.pivot_filter_threshold) {
		// use a set of filtered aggregates
		pivot_node = PivotFilteredAggregate(ref, std::move(all_columns), handled_columns, std::move(pivot_values));
	} else {
		// executing a pivot statement happens in three stages
		// 1) execute the query "SELECT {groups}, {pivots}, {aggregates} FROM {from_clause} GROUP BY {groups}, {pivots}
		// this computes all values that are required in the final result, but not yet in the correct orientation
		// 2) execute the query "SELECT {groups}, LIST({pivots}), LIST({aggregates}) FROM [Q1] GROUP BY {groups}
		// this pushes all pivots and aggregates that belong to a specific group together in an aligned manner
		// 3) push a PIVOT operator, that performs the actual pivoting of the values into the different columns

		PivotBindState bind_state;
		// Pivot Stage 1
		// SELECT {groups}, {pivots}, {aggregates} FROM {from_clause} GROUP BY {groups}, {pivots}
		auto subquery_stage1 = PivotInitialAggregate(bind_state, ref, std::move(all_columns), handled_columns);

		// Pivot stage 2
		// SELECT {groups}, LIST({pivots}), LIST({aggregates}) FROM [Q1] GROUP BY {groups}
		auto subquery_stage2 = PivotListAggregate(bind_state, ref, std::move(subquery_stage1));

		// Pivot stage 3
		// construct the final pivot operator
		pivot_node = PivotFinalOperator(bind_state, ref, std::move(subquery_stage2), std::move(pivot_values));
	}
	return pivot_node;
}

unique_ptr<SelectNode> Binder::BindUnpivot(Binder &child_binder, PivotRef &ref,
                                           vector<unique_ptr<ParsedExpression>> all_columns,
                                           unique_ptr<ParsedExpression> &where_clause) {
	D_ASSERT(ref.groups.empty());
	D_ASSERT(ref.pivots.size() == 1);

	unique_ptr<ParsedExpression> expr;
	auto select_node = make_uniq<SelectNode>();
	select_node->from_table = std::move(ref.source);

	// handle the pivot
	auto &unpivot = ref.pivots[0];

	// handle star expressions in any entries
	vector<PivotColumnEntry> new_entries;
	for (auto &entry : unpivot.entries) {
		if (entry.star_expr) {
			D_ASSERT(entry.values.empty());
			vector<unique_ptr<ParsedExpression>> star_columns;
			child_binder.ExpandStarExpression(std::move(entry.star_expr), star_columns);

			for (auto &col : star_columns) {
				if (col->type != ExpressionType::COLUMN_REF) {
					throw InternalException("Unexpected child of unpivot star - not a ColumnRef");
				}
				auto &columnref = col->Cast<ColumnRefExpression>();
				PivotColumnEntry new_entry;
				new_entry.values.emplace_back(columnref.GetColumnName());
				new_entry.alias = columnref.GetColumnName();
				new_entries.push_back(std::move(new_entry));
			}
		} else {
			new_entries.push_back(std::move(entry));
		}
	}
	unpivot.entries = std::move(new_entries);

	case_insensitive_set_t handled_columns;
	case_insensitive_map_t<string> name_map;
	for (auto &entry : unpivot.entries) {
		for (auto &value : entry.values) {
			handled_columns.insert(value.ToString());
		}
	}

	for (auto &col_expr : all_columns) {
		if (col_expr->type != ExpressionType::COLUMN_REF) {
			throw InternalException("Unexpected child of pivot source - not a ColumnRef");
		}
		auto &columnref = col_expr->Cast<ColumnRefExpression>();
		auto &column_name = columnref.GetColumnName();
		auto entry = handled_columns.find(column_name);
		if (entry == handled_columns.end()) {
			// not handled - add to the set of regularly selected columns
			select_node->select_list.push_back(std::move(col_expr));
		} else {
			name_map[column_name] = column_name;
			handled_columns.erase(entry);
		}
	}
	if (!handled_columns.empty()) {
		for (auto &entry : handled_columns) {
			throw BinderException("Column \"%s\" referenced in UNPIVOT but no matching entry was found in the table",
			                      entry);
		}
	}
	vector<Value> unpivot_names;
	for (auto &entry : unpivot.entries) {
		string generated_name;
		for (auto &val : entry.values) {
			auto name_entry = name_map.find(val.ToString());
			if (name_entry == name_map.end()) {
				throw InternalException("Unpivot - could not find column name in name map");
			}
			if (!generated_name.empty()) {
				generated_name += "_";
			}
			generated_name += name_entry->second;
		}
		unpivot_names.emplace_back(!entry.alias.empty() ? entry.alias : generated_name);
	}
	vector<vector<unique_ptr<ParsedExpression>>> unpivot_expressions;
	for (idx_t v_idx = 1; v_idx < unpivot.entries.size(); v_idx++) {
		if (unpivot.entries[v_idx].values.size() != unpivot.entries[0].values.size()) {
			throw BinderException(
			    "UNPIVOT value count mismatch - entry has %llu values, but expected all entries to have %llu values",
			    unpivot.entries[v_idx].values.size(), unpivot.entries[0].values.size());
		}
	}

	for (idx_t v_idx = 0; v_idx < unpivot.entries[0].values.size(); v_idx++) {
		vector<unique_ptr<ParsedExpression>> expressions;
		expressions.reserve(unpivot.entries.size());
		for (auto &entry : unpivot.entries) {
			expressions.push_back(make_uniq<ColumnRefExpression>(entry.values[v_idx].ToString()));
		}
		unpivot_expressions.push_back(std::move(expressions));
	}

	// construct the UNNEST expression for the set of names (constant)
	auto unpivot_list = Value::LIST(LogicalType::VARCHAR, std::move(unpivot_names));
	auto unpivot_name_expr = make_uniq<ConstantExpression>(std::move(unpivot_list));
	vector<unique_ptr<ParsedExpression>> unnest_name_children;
	unnest_name_children.push_back(std::move(unpivot_name_expr));
	auto unnest_name_expr = make_uniq<FunctionExpression>("unnest", std::move(unnest_name_children));
	unnest_name_expr->alias = unpivot.unpivot_names[0];
	select_node->select_list.push_back(std::move(unnest_name_expr));

	// construct the UNNEST expression for the set of unpivoted columns
	if (ref.unpivot_names.size() != unpivot_expressions.size()) {
		throw BinderException("UNPIVOT name count mismatch - got %d names but %d expressions", ref.unpivot_names.size(),
		                      unpivot_expressions.size());
	}
	for (idx_t i = 0; i < unpivot_expressions.size(); i++) {
		auto list_expr = make_uniq<FunctionExpression>("list_value", std::move(unpivot_expressions[i]));
		vector<unique_ptr<ParsedExpression>> unnest_val_children;
		unnest_val_children.push_back(std::move(list_expr));
		auto unnest_val_expr = make_uniq<FunctionExpression>("unnest", std::move(unnest_val_children));
		auto unnest_name = i < ref.column_name_alias.size() ? ref.column_name_alias[i] : ref.unpivot_names[i];
		unnest_val_expr->alias = unnest_name;
		select_node->select_list.push_back(std::move(unnest_val_expr));
		if (!ref.include_nulls) {
			// if we are running with EXCLUDE NULLS we need to add an IS NOT NULL filter
			auto colref = make_uniq<ColumnRefExpression>(unnest_name);
			auto filter = make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(colref));
			if (where_clause) {
				where_clause = make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
				                                                std::move(where_clause), std::move(filter));
			} else {
				where_clause = std::move(filter);
			}
		}
	}
	return select_node;
}

unique_ptr<BoundTableRef> Binder::Bind(PivotRef &ref) {
	if (!ref.source) {
		throw InternalException("Pivot without a source!?");
	}
	if (!ref.bound_pivot_values.empty() || !ref.bound_group_names.empty() || !ref.bound_aggregate_names.empty()) {
		// bound pivot
		return BindBoundPivot(ref);
	}

	// bind the source of the pivot
	// we need to do this to be able to expand star expressions
	if (ref.source->type == TableReferenceType::SUBQUERY && ref.source->alias.empty()) {
		ref.source->alias = "__internal_pivot_alias_" + to_string(GenerateTableIndex());
	}
	auto copied_source = ref.source->Copy();
	auto star_binder = Binder::CreateBinder(context, this);
	star_binder->Bind(*copied_source);

	// figure out the set of column names that are in the source of the pivot
	vector<unique_ptr<ParsedExpression>> all_columns;
	star_binder->ExpandStarExpression(make_uniq<StarExpression>(), all_columns);

	unique_ptr<SelectNode> select_node;
	unique_ptr<ParsedExpression> where_clause;
	if (!ref.aggregates.empty()) {
		select_node = BindPivot(ref, std::move(all_columns));
	} else {
		select_node = BindUnpivot(*star_binder, ref, std::move(all_columns), where_clause);
	}
	// bind the generated select node
	auto child_binder = Binder::CreateBinder(context, this);
	auto bound_select_node = child_binder->BindNode(*select_node);
	auto root_index = bound_select_node->GetRootIndex();
	BoundQueryNode *bound_select_ptr = bound_select_node.get();

	unique_ptr<BoundTableRef> result;
	MoveCorrelatedExpressions(*child_binder);
	result = make_uniq<BoundSubqueryRef>(std::move(child_binder), std::move(bound_select_node));
	auto subquery_alias = ref.alias.empty() ? "__unnamed_pivot" : ref.alias;
	SubqueryRef subquery_ref(nullptr, subquery_alias);
	subquery_ref.column_name_alias = std::move(ref.column_name_alias);
	if (where_clause) {
		// if a WHERE clause was provided - bind a subquery holding the WHERE clause
		// we need to bind a new subquery here because the WHERE clause has to be applied AFTER the unnest
		child_binder = Binder::CreateBinder(context, this);
		child_binder->bind_context.AddSubquery(root_index, subquery_ref.alias, subquery_ref, *bound_select_ptr);
		auto where_query = make_uniq<SelectNode>();
		where_query->select_list.push_back(make_uniq<StarExpression>());
		where_query->where_clause = std::move(where_clause);
		bound_select_node = child_binder->BindSelectNode(*where_query, std::move(result));
		bound_select_ptr = bound_select_node.get();
		root_index = bound_select_node->GetRootIndex();
		result = make_uniq<BoundSubqueryRef>(std::move(child_binder), std::move(bound_select_node));
	}
	bind_context.AddSubquery(root_index, subquery_ref.alias, subquery_ref, *bound_select_ptr);
	return result;
}

} // namespace duckdb




namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(SubqueryRef &ref, optional_ptr<CommonTableExpressionInfo> cte) {
	auto binder = Binder::CreateBinder(context, this);
	binder->can_contain_nulls = true;
	if (cte) {
		binder->bound_ctes.insert(*cte);
	}
	binder->alias = ref.alias.empty() ? "unnamed_subquery" : ref.alias;
	auto subquery = binder->BindNode(*ref.subquery->node);
	idx_t bind_index = subquery->GetRootIndex();
	string subquery_alias;
	if (ref.alias.empty()) {
		subquery_alias = "unnamed_subquery" + to_string(bind_index);
	} else {
		subquery_alias = ref.alias;
	}
	auto result = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(subquery));
	bind_context.AddSubquery(bind_index, subquery_alias, ref, *result->subquery);
	MoveCorrelatedExpressions(*result->binder);
	return std::move(result);
}

} // namespace duckdb























namespace duckdb {

static bool IsTableInTableOutFunction(TableFunctionCatalogEntry &table_function) {
	auto fun = table_function.functions.GetFunctionByOffset(0);
	return table_function.functions.Size() == 1 && fun.arguments.size() == 1 &&
	       fun.arguments[0].id() == LogicalTypeId::TABLE;
}

bool Binder::BindTableInTableOutFunction(vector<unique_ptr<ParsedExpression>> &expressions,
                                         unique_ptr<BoundSubqueryRef> &subquery, string &error) {
	auto binder = Binder::CreateBinder(this->context, this, true);
	unique_ptr<QueryNode> subquery_node;
	if (expressions.size() == 1 && expressions[0]->type == ExpressionType::SUBQUERY) {
		// general case: argument is a subquery, bind it as part of the node
		auto &se = expressions[0]->Cast<SubqueryExpression>();
		subquery_node = std::move(se.subquery->node);
	} else {
		// special case: non-subquery parameter to table-in table-out function
		// generate a subquery and bind that (i.e. UNNEST([1,2,3]) becomes UNNEST((SELECT [1,2,3]))
		auto select_node = make_uniq<SelectNode>();
		select_node->select_list = std::move(expressions);
		select_node->from_table = make_uniq<EmptyTableRef>();
		subquery_node = std::move(select_node);
	}
	auto node = binder->BindNode(*subquery_node);
	subquery = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(node));
	MoveCorrelatedExpressions(*subquery->binder);
	return true;
}

bool Binder::BindTableFunctionParameters(TableFunctionCatalogEntry &table_function,
                                         vector<unique_ptr<ParsedExpression>> &expressions,
                                         vector<LogicalType> &arguments, vector<Value> &parameters,
                                         named_parameter_map_t &named_parameters,
                                         unique_ptr<BoundSubqueryRef> &subquery, string &error) {
	if (IsTableInTableOutFunction(table_function)) {
		// special case binding for table-in table-out function
		arguments.emplace_back(LogicalTypeId::TABLE);
		return BindTableInTableOutFunction(expressions, subquery, error);
	}
	bool seen_subquery = false;
	for (auto &child : expressions) {
		string parameter_name;

		// hack to make named parameters work
		if (child->type == ExpressionType::COMPARE_EQUAL) {
			// comparison, check if the LHS is a columnref
			auto &comp = child->Cast<ComparisonExpression>();
			if (comp.left->type == ExpressionType::COLUMN_REF) {
				auto &colref = comp.left->Cast<ColumnRefExpression>();
				if (!colref.IsQualified()) {
					parameter_name = colref.GetColumnName();
					child = std::move(comp.right);
				}
			}
		}
		if (child->type == ExpressionType::SUBQUERY) {
			auto fun = table_function.functions.GetFunctionByOffset(0);
			if (table_function.functions.Size() != 1 || fun.arguments.empty() ||
			    fun.arguments[0].id() != LogicalTypeId::TABLE) {
				throw BinderException(
				    "Only table-in-out functions can have subquery parameters - %s only accepts constant parameters",
				    fun.name);
			}
			// this separate subquery binding path is only used by python_map
			// FIXME: this should be unified with `BindTableInTableOutFunction` above
			if (seen_subquery) {
				error = "Table function can have at most one subquery parameter ";
				return false;
			}
			auto binder = Binder::CreateBinder(this->context, this, true);
			auto &se = child->Cast<SubqueryExpression>();
			auto node = binder->BindNode(*se.subquery->node);
			subquery = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(node));
			seen_subquery = true;
			arguments.emplace_back(LogicalTypeId::TABLE);
			parameters.emplace_back(
			    Value(LogicalType::INVALID)); // this is a dummy value so the lengths of arguments and parameter match
			continue;
		}

		TableFunctionBinder binder(*this, context);
		LogicalType sql_type;
		auto expr = binder.Bind(child, &sql_type);
		if (expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		if (!expr->IsScalar()) {
			// should have been eliminated before
			throw InternalException("Table function requires a constant parameter");
		}
		auto constant = ExpressionExecutor::EvaluateScalar(context, *expr, true);
		if (parameter_name.empty()) {
			// unnamed parameter
			if (!named_parameters.empty()) {
				error = "Unnamed parameters cannot come after named parameters";
				return false;
			}
			arguments.emplace_back(sql_type);
			parameters.emplace_back(std::move(constant));
		} else {
			named_parameters[parameter_name] = std::move(constant);
		}
	}
	return true;
}

unique_ptr<LogicalOperator>
Binder::BindTableFunctionInternal(TableFunction &table_function, const string &function_name, vector<Value> parameters,
                                  named_parameter_map_t named_parameters, vector<LogicalType> input_table_types,
                                  vector<string> input_table_names, const vector<string> &column_name_alias,
                                  unique_ptr<ExternalDependency> external_dependency) {
	auto bind_index = GenerateTableIndex();
	// perform the binding
	unique_ptr<FunctionData> bind_data;
	vector<LogicalType> return_types;
	vector<string> return_names;
	if (table_function.bind || table_function.bind_replace) {
		TableFunctionBindInput bind_input(parameters, named_parameters, input_table_types, input_table_names,
		                                  table_function.function_info.get());
		if (table_function.bind_replace) {
			auto new_plan = table_function.bind_replace(context, bind_input);
			if (new_plan != nullptr) {
				return CreatePlan(*Bind(*new_plan));
			} else if (!table_function.bind) {
				throw BinderException("Failed to bind \"%s\": nullptr returned from bind_replace without bind function",
				                      table_function.name);
			}
		}
		bind_data = table_function.bind(context, bind_input, return_types, return_names);
		if (table_function.name == "pandas_scan" || table_function.name == "arrow_scan") {
			auto &arrow_bind = bind_data->Cast<PyTableFunctionData>();
			arrow_bind.external_dependency = std::move(external_dependency);
		}
		if (table_function.name == "read_csv" || table_function.name == "read_csv_auto") {
			auto &csv_bind = bind_data->Cast<ReadCSVData>();
			if (csv_bind.single_threaded) {
				table_function.extra_info = "(Single-Threaded)";
			} else {
				table_function.extra_info = "(Multi-Threaded)";
			}
		}
	} else {
		throw InvalidInputException("Cannot call function \"%s\" directly - it has no bind function",
		                            table_function.name);
	}
	if (return_types.size() != return_names.size()) {
		throw InternalException("Failed to bind \"%s\": return_types/names must have same size", table_function.name);
	}
	if (return_types.empty()) {
		throw InternalException("Failed to bind \"%s\": Table function must return at least one column",
		                        table_function.name);
	}
	// overwrite the names with any supplied aliases
	for (idx_t i = 0; i < column_name_alias.size() && i < return_names.size(); i++) {
		return_names[i] = column_name_alias[i];
	}
	for (idx_t i = 0; i < return_names.size(); i++) {
		if (return_names[i].empty()) {
			return_names[i] = "C" + to_string(i);
		}
	}

	auto get = make_uniq<LogicalGet>(bind_index, table_function, std::move(bind_data), return_types, return_names);
	get->parameters = parameters;
	get->named_parameters = named_parameters;
	get->input_table_types = input_table_types;
	get->input_table_names = input_table_names;
	if (table_function.in_out_function && !table_function.projection_pushdown) {
		get->column_ids.reserve(return_types.size());
		for (idx_t i = 0; i < return_types.size(); i++) {
			get->column_ids.push_back(i);
		}
	}
	// now add the table function to the bind context so its columns can be bound
	bind_context.AddTableFunction(bind_index, function_name, return_names, return_types, get->column_ids,
	                              get->GetTable().get());
	return std::move(get);
}

unique_ptr<LogicalOperator> Binder::BindTableFunction(TableFunction &function, vector<Value> parameters) {
	named_parameter_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<string> input_table_names;
	vector<string> column_name_aliases;
	return BindTableFunctionInternal(function, function.name, std::move(parameters), std::move(named_parameters),
	                                 std::move(input_table_types), std::move(input_table_names), column_name_aliases,
	                                 nullptr);
}

unique_ptr<BoundTableRef> Binder::Bind(TableFunctionRef &ref) {
	QueryErrorContext error_context(root_statement, ref.query_location);

	D_ASSERT(ref.function->type == ExpressionType::FUNCTION);
	auto &fexpr = ref.function->Cast<FunctionExpression>();

	// fetch the function from the catalog
	auto &func_catalog = Catalog::GetEntry(context, CatalogType::TABLE_FUNCTION_ENTRY, fexpr.catalog, fexpr.schema,
	                                       fexpr.function_name, error_context);

	if (func_catalog.type == CatalogType::TABLE_MACRO_ENTRY) {
		auto &macro_func = func_catalog.Cast<TableMacroCatalogEntry>();
		auto query_node = BindTableMacro(fexpr, macro_func, 0);
		D_ASSERT(query_node);

		auto binder = Binder::CreateBinder(context, this);
		binder->can_contain_nulls = true;

		binder->alias = ref.alias.empty() ? "unnamed_query" : ref.alias;
		auto query = binder->BindNode(*query_node);

		idx_t bind_index = query->GetRootIndex();
		// string alias;
		string alias = (ref.alias.empty() ? "unnamed_query" + to_string(bind_index) : ref.alias);

		auto result = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(query));
		// remember ref here is TableFunctionRef and NOT base class
		bind_context.AddSubquery(bind_index, alias, ref, *result->subquery);
		MoveCorrelatedExpressions(*result->binder);
		return std::move(result);
	}
	D_ASSERT(func_catalog.type == CatalogType::TABLE_FUNCTION_ENTRY);
	auto &function = func_catalog.Cast<TableFunctionCatalogEntry>();

	// evaluate the input parameters to the function
	vector<LogicalType> arguments;
	vector<Value> parameters;
	named_parameter_map_t named_parameters;
	unique_ptr<BoundSubqueryRef> subquery;
	string error;
	if (!BindTableFunctionParameters(function, fexpr.children, arguments, parameters, named_parameters, subquery,
	                                 error)) {
		throw BinderException(FormatError(ref, error));
	}

	// select the function based on the input parameters
	FunctionBinder function_binder(context);
	idx_t best_function_idx = function_binder.BindFunction(function.name, function.functions, arguments, error);
	if (best_function_idx == DConstants::INVALID_INDEX) {
		throw BinderException(FormatError(ref, error));
	}
	auto table_function = function.functions.GetFunctionByOffset(best_function_idx);

	// now check the named parameters
	BindNamedParameters(table_function.named_parameters, named_parameters, error_context, table_function.name);

	// cast the parameters to the type of the function
	for (idx_t i = 0; i < arguments.size(); i++) {
		auto target_type = i < table_function.arguments.size() ? table_function.arguments[i] : table_function.varargs;

		if (target_type != LogicalType::ANY && target_type != LogicalType::TABLE &&
		    target_type != LogicalType::POINTER && target_type.id() != LogicalTypeId::LIST) {
			parameters[i] = parameters[i].CastAs(context, target_type);
		}
	}

	vector<LogicalType> input_table_types;
	vector<string> input_table_names;

	if (subquery) {
		input_table_types = subquery->subquery->types;
		input_table_names = subquery->subquery->names;
	}
	auto get = BindTableFunctionInternal(table_function, ref.alias.empty() ? fexpr.function_name : ref.alias,
	                                     std::move(parameters), std::move(named_parameters),
	                                     std::move(input_table_types), std::move(input_table_names),
	                                     ref.column_name_alias, std::move(ref.external_dependency));
	if (subquery) {
		get->children.push_back(Binder::CreatePlan(*subquery));
	}

	return make_uniq_base<BoundTableRef, BoundTableFunction>(std::move(get));
}

} // namespace duckdb




namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundBaseTableRef &ref) {
	return std::move(ref.get);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTERef &ref) {
	auto index = ref.bind_index;

	vector<LogicalType> types;
	types.reserve(ref.types.size());
	for (auto &type : ref.types) {
		types.push_back(type);
	}

	return make_uniq<LogicalCTERef>(index, ref.cte_index, types, ref.bound_columns, ref.materialized_cte);
}

} // namespace duckdb




namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundEmptyTableRef &ref) {
	return make_uniq<LogicalDummyScan>(ref.bind_index);
}

} // namespace duckdb





namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundExpressionListRef &ref) {
	auto root = make_uniq_base<LogicalOperator, LogicalDummyScan>(GenerateTableIndex());
	// values list, first plan any subqueries in the list
	for (auto &expr_list : ref.values) {
		for (auto &expr : expr_list) {
			PlanSubqueries(expr, root);
		}
	}
	// now create a LogicalExpressionGet from the set of expressions
	// fetch the types
	vector<LogicalType> types;
	for (auto &expr : ref.values[0]) {
		types.push_back(expr->return_type);
	}
	auto expr_get = make_uniq<LogicalExpressionGet>(ref.bind_index, types, std::move(ref.values));
	expr_get->AddChild(std::move(root));
	return std::move(expr_get);
}

} // namespace duckdb




















namespace duckdb {

//! Create a JoinCondition from a comparison
static bool CreateJoinCondition(Expression &expr, const unordered_set<idx_t> &left_bindings,
                                const unordered_set<idx_t> &right_bindings, vector<JoinCondition> &conditions) {
	// comparison
	auto &comparison = expr.Cast<BoundComparisonExpression>();
	auto left_side = JoinSide::GetJoinSide(*comparison.left, left_bindings, right_bindings);
	auto right_side = JoinSide::GetJoinSide(*comparison.right, left_bindings, right_bindings);
	if (left_side != JoinSide::BOTH && right_side != JoinSide::BOTH) {
		// join condition can be divided in a left/right side
		JoinCondition condition;
		condition.comparison = expr.type;
		auto left = std::move(comparison.left);
		auto right = std::move(comparison.right);
		if (left_side == JoinSide::RIGHT) {
			// left = right, right = left, flip the comparison symbol and reverse sides
			swap(left, right);
			condition.comparison = FlipComparisonExpression(expr.type);
		}
		condition.left = std::move(left);
		condition.right = std::move(right);
		conditions.push_back(std::move(condition));
		return true;
	}
	return false;
}

void LogicalComparisonJoin::ExtractJoinConditions(
    ClientContext &context, JoinType type, unique_ptr<LogicalOperator> &left_child,
    unique_ptr<LogicalOperator> &right_child, const unordered_set<idx_t> &left_bindings,
    const unordered_set<idx_t> &right_bindings, vector<unique_ptr<Expression>> &expressions,
    vector<JoinCondition> &conditions, vector<unique_ptr<Expression>> &arbitrary_expressions) {

	for (auto &expr : expressions) {
		auto total_side = JoinSide::GetJoinSide(*expr, left_bindings, right_bindings);
		if (total_side != JoinSide::BOTH) {
			// join condition does not reference both sides, add it as filter under the join
			if (type == JoinType::LEFT && total_side == JoinSide::RIGHT) {
				// filter is on RHS and the join is a LEFT OUTER join, we can push it in the right child
				if (right_child->type != LogicalOperatorType::LOGICAL_FILTER) {
					// not a filter yet, push a new empty filter
					auto filter = make_uniq<LogicalFilter>();
					filter->AddChild(std::move(right_child));
					right_child = std::move(filter);
				}
				// push the expression into the filter
				auto &filter = right_child->Cast<LogicalFilter>();
				filter.expressions.push_back(std::move(expr));
				continue;
			}
			// if the join is a LEFT JOIN and the join expression constantly evaluates to TRUE,
			// then we do not add it to the arbitrary expressions
			if (type == JoinType::LEFT && expr->IsFoldable()) {
				Value result;
				ExpressionExecutor::TryEvaluateScalar(context, *expr, result);
				if (!result.IsNull() && result == Value(true)) {
					continue;
				}
			}
		} else if (expr->type == ExpressionType::COMPARE_EQUAL || expr->type == ExpressionType::COMPARE_NOTEQUAL ||
		           expr->type == ExpressionType::COMPARE_BOUNDARY_START ||
		           expr->type == ExpressionType::COMPARE_LESSTHAN ||
		           expr->type == ExpressionType::COMPARE_GREATERTHAN ||
		           expr->type == ExpressionType::COMPARE_LESSTHANOREQUALTO ||
		           expr->type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		           expr->type == ExpressionType::COMPARE_BOUNDARY_START ||
		           expr->type == ExpressionType::COMPARE_NOT_DISTINCT_FROM ||
		           expr->type == ExpressionType::COMPARE_DISTINCT_FROM)

		{
			// comparison, check if we can create a comparison JoinCondition
			if (CreateJoinCondition(*expr, left_bindings, right_bindings, conditions)) {
				// successfully created the join condition
				continue;
			}
		}
		arbitrary_expressions.push_back(std::move(expr));
	}
}

void LogicalComparisonJoin::ExtractJoinConditions(ClientContext &context, JoinType type,
                                                  unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  vector<unique_ptr<Expression>> &expressions,
                                                  vector<JoinCondition> &conditions,
                                                  vector<unique_ptr<Expression>> &arbitrary_expressions) {
	unordered_set<idx_t> left_bindings, right_bindings;
	LogicalJoin::GetTableReferences(*left_child, left_bindings);
	LogicalJoin::GetTableReferences(*right_child, right_bindings);
	return ExtractJoinConditions(context, type, left_child, right_child, left_bindings, right_bindings, expressions,
	                             conditions, arbitrary_expressions);
}

void LogicalComparisonJoin::ExtractJoinConditions(ClientContext &context, JoinType type,
                                                  unique_ptr<LogicalOperator> &left_child,
                                                  unique_ptr<LogicalOperator> &right_child,
                                                  unique_ptr<Expression> condition, vector<JoinCondition> &conditions,
                                                  vector<unique_ptr<Expression>> &arbitrary_expressions) {
	// split the expressions by the AND clause
	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(std::move(condition));
	LogicalFilter::SplitPredicates(expressions);
	return ExtractJoinConditions(context, type, left_child, right_child, expressions, conditions,
	                             arbitrary_expressions);
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(ClientContext &context, JoinType type,
                                                              JoinRefType reftype,
                                                              unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              vector<JoinCondition> conditions,
                                                              vector<unique_ptr<Expression>> arbitrary_expressions) {
	// Validate the conditions
	bool need_to_consider_arbitrary_expressions = true;
	switch (reftype) {
	case JoinRefType::ASOF: {
		need_to_consider_arbitrary_expressions = false;
		auto asof_idx = conditions.size();
		for (size_t c = 0; c < conditions.size(); ++c) {
			auto &cond = conditions[c];
			switch (cond.comparison) {
			case ExpressionType::COMPARE_EQUAL:
			case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
				break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			case ExpressionType::COMPARE_GREATERTHAN:
			case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			case ExpressionType::COMPARE_LESSTHAN:
				if (asof_idx < conditions.size()) {
					throw BinderException("Multiple ASOF JOIN inequalities");
				}
				asof_idx = c;
				break;
			default:
				throw BinderException("Invalid ASOF JOIN comparison");
			}
		}
		if (asof_idx == conditions.size()) {
			throw BinderException("Missing ASOF JOIN inequality");
		}
		break;
	}
	default:
		break;
	}

	if (type == JoinType::INNER && reftype == JoinRefType::REGULAR) {
		// for inner joins we can push arbitrary expressions as a filter
		// here we prefer to create a comparison join if possible
		// that way we can use the much faster hash join to process the main join
		// rather than doing a nested loop join to handle arbitrary expressions

		// for left and full outer joins we HAVE to process all join conditions
		// because pushing a filter will lead to an incorrect result, as non-matching tuples cannot be filtered out
		need_to_consider_arbitrary_expressions = false;
	}
	if ((need_to_consider_arbitrary_expressions && !arbitrary_expressions.empty()) || conditions.empty()) {
		if (arbitrary_expressions.empty()) {
			// all conditions were pushed down, add TRUE predicate
			arbitrary_expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
		}
		for (auto &condition : conditions) {
			arbitrary_expressions.push_back(JoinCondition::CreateExpression(std::move(condition)));
		}
		// if we get here we could not create any JoinConditions
		// turn this into an arbitrary expression join
		auto any_join = make_uniq<LogicalAnyJoin>(type);
		// create the condition
		any_join->children.push_back(std::move(left_child));
		any_join->children.push_back(std::move(right_child));
		// AND all the arbitrary expressions together
		// do the same with any remaining conditions
		any_join->condition = std::move(arbitrary_expressions[0]);
		for (idx_t i = 1; i < arbitrary_expressions.size(); i++) {
			any_join->condition = make_uniq<BoundConjunctionExpression>(
			    ExpressionType::CONJUNCTION_AND, std::move(any_join->condition), std::move(arbitrary_expressions[i]));
		}
		return std::move(any_join);
	} else {
		// we successfully converted expressions into JoinConditions
		// create a LogicalComparisonJoin
		auto logical_type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
		if (reftype == JoinRefType::ASOF) {
			logical_type = LogicalOperatorType::LOGICAL_ASOF_JOIN;
		}
		auto comp_join = make_uniq<LogicalComparisonJoin>(type, logical_type);
		comp_join->conditions = std::move(conditions);
		comp_join->children.push_back(std::move(left_child));
		comp_join->children.push_back(std::move(right_child));
		if (!arbitrary_expressions.empty()) {
			// we have some arbitrary expressions as well
			// add them to a filter
			auto filter = make_uniq<LogicalFilter>();
			for (auto &expr : arbitrary_expressions) {
				filter->expressions.push_back(std::move(expr));
			}
			LogicalFilter::SplitPredicates(filter->expressions);
			filter->children.push_back(std::move(comp_join));
			return std::move(filter);
		}
		return std::move(comp_join);
	}
}

static bool HasCorrelatedColumns(Expression &expression) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		if (colref.depth > 0) {
			return true;
		}
	}
	bool has_correlated_columns = false;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		if (HasCorrelatedColumns(child)) {
			has_correlated_columns = true;
		}
	});
	return has_correlated_columns;
}

unique_ptr<LogicalOperator> LogicalComparisonJoin::CreateJoin(ClientContext &context, JoinType type,
                                                              JoinRefType reftype,
                                                              unique_ptr<LogicalOperator> left_child,
                                                              unique_ptr<LogicalOperator> right_child,
                                                              unique_ptr<Expression> condition) {
	vector<JoinCondition> conditions;
	vector<unique_ptr<Expression>> arbitrary_expressions;
	LogicalComparisonJoin::ExtractJoinConditions(context, type, left_child, right_child, std::move(condition),
	                                             conditions, arbitrary_expressions);
	return LogicalComparisonJoin::CreateJoin(context, type, reftype, std::move(left_child), std::move(right_child),
	                                         std::move(conditions), std::move(arbitrary_expressions));
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundJoinRef &ref) {
	auto old_is_outside_flattened = is_outside_flattened;
	// Plan laterals from outermost to innermost
	if (ref.lateral) {
		// Set the flag to ensure that children do not flatten before the root
		is_outside_flattened = false;
	}
	auto left = CreatePlan(*ref.left);
	auto right = CreatePlan(*ref.right);
	is_outside_flattened = old_is_outside_flattened;

	// For joins, depth of the bindings will be one higher on the right because of the lateral binder
	// If the current join does not have correlations between left and right, then the right bindings
	// have depth 1 too high and can be reduced by 1 throughout
	if (!ref.lateral && !ref.correlated_columns.empty()) {
		LateralBinder::ReduceExpressionDepth(*right, ref.correlated_columns);
	}

	if (ref.type == JoinType::RIGHT && ref.ref_type != JoinRefType::ASOF &&
	    ClientConfig::GetConfig(context).enable_optimizer) {
		// we turn any right outer joins into left outer joins for optimization purposes
		// they are the same but with sides flipped, so treating them the same simplifies life
		ref.type = JoinType::LEFT;
		std::swap(left, right);
	}
	if (ref.lateral) {
		if (!is_outside_flattened) {
			// If outer dependent joins is yet to be flattened, only plan the lateral
			has_unplanned_dependent_joins = true;
			return LogicalDependentJoin::Create(std::move(left), std::move(right), ref.correlated_columns, ref.type,
			                                    std::move(ref.condition));
		} else {
			// All outer dependent joins have been planned and flattened, so plan and flatten lateral and recursively
			// plan the children
			auto new_plan = PlanLateralJoin(std::move(left), std::move(right), ref.correlated_columns, ref.type,
			                                std::move(ref.condition));
			if (has_unplanned_dependent_joins) {
				RecursiveDependentJoinPlanner plan(*this);
				plan.VisitOperator(*new_plan);
			}
			return new_plan;
		}
	}
	switch (ref.ref_type) {
	case JoinRefType::CROSS:
		return LogicalCrossProduct::Create(std::move(left), std::move(right));
	case JoinRefType::POSITIONAL:
		return LogicalPositionalJoin::Create(std::move(left), std::move(right));
	default:
		break;
	}
	if (ref.type == JoinType::INNER && (ref.condition->HasSubquery() || HasCorrelatedColumns(*ref.condition)) &&
	    ref.ref_type == JoinRefType::REGULAR) {
		// inner join, generate a cross product + filter
		// this will be later turned into a proper join by the join order optimizer
		auto root = LogicalCrossProduct::Create(std::move(left), std::move(right));

		auto filter = make_uniq<LogicalFilter>(std::move(ref.condition));
		// visit the expressions in the filter
		for (auto &expression : filter->expressions) {
			PlanSubqueries(expression, root);
		}
		filter->AddChild(std::move(root));
		return std::move(filter);
	}

	// now create the join operator from the join condition
	auto result = LogicalComparisonJoin::CreateJoin(context, ref.type, ref.ref_type, std::move(left), std::move(right),
	                                                std::move(ref.condition));

	optional_ptr<LogicalOperator> join;
	if (result->type == LogicalOperatorType::LOGICAL_FILTER) {
		join = result->children[0].get();
	} else {
		join = result.get();
	}
	for (auto &child : join->children) {
		if (child->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = child->Cast<LogicalFilter>();
			for (auto &expr : filter.expressions) {
				PlanSubqueries(expr, filter.children[0]);
			}
		}
	}

	// we visit the expressions depending on the type of join
	switch (join->type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		// comparison join
		// in this join we visit the expressions on the LHS with the LHS as root node
		// and the expressions on the RHS with the RHS as root node
		auto &comp_join = join->Cast<LogicalComparisonJoin>();
		for (idx_t i = 0; i < comp_join.conditions.size(); i++) {
			PlanSubqueries(comp_join.conditions[i].left, comp_join.children[0]);
			PlanSubqueries(comp_join.conditions[i].right, comp_join.children[1]);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &any_join = join->Cast<LogicalAnyJoin>();
		// for the any join we just visit the condition
		if (any_join.condition->HasSubquery()) {
			throw NotImplementedException("Cannot perform non-inner join on subquery!");
		}
		break;
	}
	default:
		break;
	}
	return result;
}

} // namespace duckdb



namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundPivotRef &ref) {
	auto subquery = ref.child_binder->CreatePlan(*ref.child);

	auto result = make_uniq<LogicalPivot>(ref.bind_index, std::move(subquery), std::move(ref.bound_pivot));
	return std::move(result);
}

} // namespace duckdb



namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSubqueryRef &ref) {
	// generate the logical plan for the subquery
	// this happens separately from the current LogicalPlan generation
	ref.binder->is_outside_flattened = is_outside_flattened;
	auto subquery = ref.binder->CreatePlan(*ref.subquery);
	if (ref.binder->has_unplanned_dependent_joins) {
		has_unplanned_dependent_joins = true;
	}
	return subquery;
}

} // namespace duckdb



namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableFunction &ref) {
	return std::move(ref.get);
}

} // namespace duckdb



















#include <algorithm>

namespace duckdb {

Binder *Binder::GetRootBinder() {
	Binder *root = this;
	while (root->parent) {
		root = root->parent.get();
	}
	return root;
}

idx_t Binder::GetBinderDepth() const {
	const Binder *root = this;
	idx_t depth = 1;
	while (root->parent) {
		depth++;
		root = root->parent.get();
	}
	return depth;
}

shared_ptr<Binder> Binder::CreateBinder(ClientContext &context, optional_ptr<Binder> parent, bool inherit_ctes) {
	auto depth = parent ? parent->GetBinderDepth() : 0;
	if (depth > context.config.max_expression_depth) {
		throw BinderException("Max expression depth limit of %lld exceeded. Use \"SET max_expression_depth TO x\" to "
		                      "increase the maximum expression depth.",
		                      context.config.max_expression_depth);
	}
	return make_shared<Binder>(true, context, parent ? parent->shared_from_this() : nullptr, inherit_ctes);
}

Binder::Binder(bool, ClientContext &context, shared_ptr<Binder> parent_p, bool inherit_ctes_p)
    : context(context), parent(std::move(parent_p)), bound_tables(0), inherit_ctes(inherit_ctes_p) {
	if (parent) {

		// We have to inherit macro and lambda parameter bindings and from the parent binder, if there is a parent.
		macro_binding = parent->macro_binding;
		lambda_bindings = parent->lambda_bindings;

		if (inherit_ctes) {
			// We have to inherit CTE bindings from the parent bind_context, if there is a parent.
			bind_context.SetCTEBindings(parent->bind_context.GetCTEBindings());
			bind_context.cte_references = parent->bind_context.cte_references;
			parameters = parent->parameters;
		}
	}
}

BoundStatement Binder::Bind(SQLStatement &statement) {
	root_statement = &statement;
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT:
		return Bind(statement.Cast<SelectStatement>());
	case StatementType::INSERT_STATEMENT:
		return Bind(statement.Cast<InsertStatement>());
	case StatementType::COPY_STATEMENT:
		return Bind(statement.Cast<CopyStatement>());
	case StatementType::DELETE_STATEMENT:
		return Bind(statement.Cast<DeleteStatement>());
	case StatementType::UPDATE_STATEMENT:
		return Bind(statement.Cast<UpdateStatement>());
	case StatementType::RELATION_STATEMENT:
		return Bind(statement.Cast<RelationStatement>());
	case StatementType::CREATE_STATEMENT:
		return Bind(statement.Cast<CreateStatement>());
	case StatementType::DROP_STATEMENT:
		return Bind(statement.Cast<DropStatement>());
	case StatementType::ALTER_STATEMENT:
		return Bind(statement.Cast<AlterStatement>());
	case StatementType::TRANSACTION_STATEMENT:
		return Bind(statement.Cast<TransactionStatement>());
	case StatementType::PRAGMA_STATEMENT:
		return Bind(statement.Cast<PragmaStatement>());
	case StatementType::EXPLAIN_STATEMENT:
		return Bind(statement.Cast<ExplainStatement>());
	case StatementType::VACUUM_STATEMENT:
		return Bind(statement.Cast<VacuumStatement>());
	case StatementType::SHOW_STATEMENT:
		return Bind(statement.Cast<ShowStatement>());
	case StatementType::CALL_STATEMENT:
		return Bind(statement.Cast<CallStatement>());
	case StatementType::EXPORT_STATEMENT:
		return Bind(statement.Cast<ExportStatement>());
	case StatementType::SET_STATEMENT:
		return Bind(statement.Cast<SetStatement>());
	case StatementType::LOAD_STATEMENT:
		return Bind(statement.Cast<LoadStatement>());
	case StatementType::EXTENSION_STATEMENT:
		return Bind(statement.Cast<ExtensionStatement>());
	case StatementType::PREPARE_STATEMENT:
		return Bind(statement.Cast<PrepareStatement>());
	case StatementType::EXECUTE_STATEMENT:
		return Bind(statement.Cast<ExecuteStatement>());
	case StatementType::LOGICAL_PLAN_STATEMENT:
		return Bind(statement.Cast<LogicalPlanStatement>());
	case StatementType::ATTACH_STATEMENT:
		return Bind(statement.Cast<AttachStatement>());
	case StatementType::DETACH_STATEMENT:
		return Bind(statement.Cast<DetachStatement>());
	default: // LCOV_EXCL_START
		throw NotImplementedException("Unimplemented statement type \"%s\" for Bind",
		                              StatementTypeToString(statement.type));
	} // LCOV_EXCL_STOP
}

void Binder::AddCTEMap(CommonTableExpressionMap &cte_map) {
	for (auto &cte_it : cte_map.map) {
		AddCTE(cte_it.first, *cte_it.second);
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(QueryNode &node) {
	// first we visit the set of CTEs and add them to the bind context
	AddCTEMap(node.cte_map);
	// now we bind the node
	unique_ptr<BoundQueryNode> result;
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		result = BindNode(node.Cast<SelectNode>());
		break;
	case QueryNodeType::RECURSIVE_CTE_NODE:
		result = BindNode(node.Cast<RecursiveCTENode>());
		break;
	case QueryNodeType::CTE_NODE:
		result = BindNode(node.Cast<CTENode>());
		break;
	default:
		D_ASSERT(node.type == QueryNodeType::SET_OPERATION_NODE);
		result = BindNode(node.Cast<SetOperationNode>());
		break;
	}
	return result;
}

BoundStatement Binder::Bind(QueryNode &node) {
	auto bound_node = BindNode(node);

	BoundStatement result;
	result.names = bound_node->names;
	result.types = bound_node->types;

	// and plan it
	result.plan = CreatePlan(*bound_node);
	return result;
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundQueryNode &node) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE:
		return CreatePlan(node.Cast<BoundSelectNode>());
	case QueryNodeType::SET_OPERATION_NODE:
		return CreatePlan(node.Cast<BoundSetOperationNode>());
	case QueryNodeType::RECURSIVE_CTE_NODE:
		return CreatePlan(node.Cast<BoundRecursiveCTENode>());
	case QueryNodeType::CTE_NODE:
		return CreatePlan(node.Cast<BoundCTENode>());
	default:
		throw InternalException("Unsupported bound query node type");
	}
}

unique_ptr<BoundTableRef> Binder::Bind(TableRef &ref) {
	unique_ptr<BoundTableRef> result;
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		result = Bind(ref.Cast<BaseTableRef>());
		break;
	case TableReferenceType::JOIN:
		result = Bind(ref.Cast<JoinRef>());
		break;
	case TableReferenceType::SUBQUERY:
		result = Bind(ref.Cast<SubqueryRef>());
		break;
	case TableReferenceType::EMPTY:
		result = Bind(ref.Cast<EmptyTableRef>());
		break;
	case TableReferenceType::TABLE_FUNCTION:
		result = Bind(ref.Cast<TableFunctionRef>());
		break;
	case TableReferenceType::EXPRESSION_LIST:
		result = Bind(ref.Cast<ExpressionListRef>());
		break;
	case TableReferenceType::PIVOT:
		result = Bind(ref.Cast<PivotRef>());
		break;
	case TableReferenceType::CTE:
	case TableReferenceType::INVALID:
	default:
		throw InternalException("Unknown table ref type");
	}
	result->sample = std::move(ref.sample);
	return result;
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundTableRef &ref) {
	unique_ptr<LogicalOperator> root;
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE:
		root = CreatePlan(ref.Cast<BoundBaseTableRef>());
		break;
	case TableReferenceType::SUBQUERY:
		root = CreatePlan(ref.Cast<BoundSubqueryRef>());
		break;
	case TableReferenceType::JOIN:
		root = CreatePlan(ref.Cast<BoundJoinRef>());
		break;
	case TableReferenceType::TABLE_FUNCTION:
		root = CreatePlan(ref.Cast<BoundTableFunction>());
		break;
	case TableReferenceType::EMPTY:
		root = CreatePlan(ref.Cast<BoundEmptyTableRef>());
		break;
	case TableReferenceType::EXPRESSION_LIST:
		root = CreatePlan(ref.Cast<BoundExpressionListRef>());
		break;
	case TableReferenceType::CTE:
		root = CreatePlan(ref.Cast<BoundCTERef>());
		break;
	case TableReferenceType::PIVOT:
		root = CreatePlan(ref.Cast<BoundPivotRef>());
		break;
	case TableReferenceType::INVALID:
	default:
		throw InternalException("Unsupported bound table ref type");
	}
	// plan the sample clause
	if (ref.sample) {
		root = make_uniq<LogicalSample>(std::move(ref.sample), std::move(root));
	}
	return root;
}

void Binder::AddCTE(const string &name, CommonTableExpressionInfo &info) {
	D_ASSERT(!name.empty());
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		throw InternalException("Duplicate CTE \"%s\" in query!", name);
	}
	CTE_bindings.insert(make_pair(name, reference<CommonTableExpressionInfo>(info)));
}

optional_ptr<CommonTableExpressionInfo> Binder::FindCTE(const string &name, bool skip) {
	auto entry = CTE_bindings.find(name);
	if (entry != CTE_bindings.end()) {
		if (!skip || entry->second.get().query->node->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			return &entry->second.get();
		}
	}
	if (parent && inherit_ctes) {
		return parent->FindCTE(name, name == alias);
	}
	return nullptr;
}

bool Binder::CTEIsAlreadyBound(CommonTableExpressionInfo &cte) {
	if (bound_ctes.find(cte) != bound_ctes.end()) {
		return true;
	}
	if (parent && inherit_ctes) {
		return parent->CTEIsAlreadyBound(cte);
	}
	return false;
}

void Binder::AddBoundView(ViewCatalogEntry &view) {
	// check if the view is already bound
	auto current = this;
	while (current) {
		if (current->bound_views.find(view) != current->bound_views.end()) {
			throw BinderException("infinite recursion detected: attempting to recursively bind view \"%s\"", view.name);
		}
		current = current->parent.get();
	}
	bound_views.insert(view);
}

idx_t Binder::GenerateTableIndex() {
	auto root_binder = GetRootBinder();
	return root_binder->bound_tables++;
}

void Binder::PushExpressionBinder(ExpressionBinder &binder) {
	GetActiveBinders().push_back(binder);
}

void Binder::PopExpressionBinder() {
	D_ASSERT(HasActiveBinder());
	GetActiveBinders().pop_back();
}

void Binder::SetActiveBinder(ExpressionBinder &binder) {
	D_ASSERT(HasActiveBinder());
	GetActiveBinders().back() = binder;
}

ExpressionBinder &Binder::GetActiveBinder() {
	return GetActiveBinders().back();
}

bool Binder::HasActiveBinder() {
	return !GetActiveBinders().empty();
}

vector<reference<ExpressionBinder>> &Binder::GetActiveBinders() {
	auto root_binder = GetRootBinder();
	return root_binder->active_binders;
}

void Binder::AddUsingBindingSet(unique_ptr<UsingColumnSet> set) {
	auto root_binder = GetRootBinder();
	root_binder->bind_context.AddUsingBindingSet(std::move(set));
}

void Binder::MoveCorrelatedExpressions(Binder &other) {
	MergeCorrelatedColumns(other.correlated_columns);
	other.correlated_columns.clear();
}

void Binder::MergeCorrelatedColumns(vector<CorrelatedColumnInfo> &other) {
	for (idx_t i = 0; i < other.size(); i++) {
		AddCorrelatedColumn(other[i]);
	}
}

void Binder::AddCorrelatedColumn(const CorrelatedColumnInfo &info) {
	// we only add correlated columns to the list if they are not already there
	if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
		correlated_columns.push_back(info);
	}
}

bool Binder::HasMatchingBinding(const string &table_name, const string &column_name, string &error_message) {
	string empty_schema;
	return HasMatchingBinding(empty_schema, table_name, column_name, error_message);
}

bool Binder::HasMatchingBinding(const string &schema_name, const string &table_name, const string &column_name,
                                string &error_message) {
	string empty_catalog;
	return HasMatchingBinding(empty_catalog, schema_name, table_name, column_name, error_message);
}

bool Binder::HasMatchingBinding(const string &catalog_name, const string &schema_name, const string &table_name,
                                const string &column_name, string &error_message) {
	optional_ptr<Binding> binding;
	D_ASSERT(!lambda_bindings);
	if (macro_binding && table_name == macro_binding->alias) {
		binding = optional_ptr<Binding>(macro_binding.get());
	} else {
		binding = bind_context.GetBinding(table_name, error_message);
	}

	if (!binding) {
		return false;
	}
	if (!catalog_name.empty() || !schema_name.empty()) {
		auto catalog_entry = binding->GetStandardEntry();
		if (!catalog_entry) {
			return false;
		}
		if (!catalog_name.empty() && catalog_entry->catalog.GetName() != catalog_name) {
			return false;
		}
		if (!schema_name.empty() && catalog_entry->schema.name != schema_name) {
			return false;
		}
		if (catalog_entry->name != table_name) {
			return false;
		}
	}
	bool binding_found;
	binding_found = binding->HasMatchingBinding(column_name);
	if (!binding_found) {
		error_message = binding->ColumnNotFoundError(column_name);
	}
	return binding_found;
}

void Binder::SetBindingMode(BindingMode mode) {
	auto root_binder = GetRootBinder();
	// FIXME: this used to also set the 'mode' for the current binder, was that necessary?
	root_binder->mode = mode;
}

BindingMode Binder::GetBindingMode() {
	auto root_binder = GetRootBinder();
	return root_binder->mode;
}

void Binder::SetCanContainNulls(bool can_contain_nulls_p) {
	can_contain_nulls = can_contain_nulls_p;
}

void Binder::AddTableName(string table_name) {
	auto root_binder = GetRootBinder();
	root_binder->table_names.insert(std::move(table_name));
}

const unordered_set<string> &Binder::GetTableNames() {
	auto root_binder = GetRootBinder();
	return root_binder->table_names;
}

string Binder::FormatError(ParsedExpression &expr_context, const string &message) {
	return FormatError(expr_context.query_location, message);
}

string Binder::FormatError(TableRef &ref_context, const string &message) {
	return FormatError(ref_context.query_location, message);
}

string Binder::FormatErrorRecursive(idx_t query_location, const string &message, vector<ExceptionFormatValue> &values) {
	QueryErrorContext context(root_statement, query_location);
	return context.FormatErrorRecursive(message, values);
}

// FIXME: this is extremely naive
void VerifyNotExcluded(ParsedExpression &expr) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &column_ref = expr.Cast<ColumnRefExpression>();
		if (!column_ref.IsQualified()) {
			return;
		}
		auto &table_name = column_ref.GetTableName();
		if (table_name == "excluded") {
			throw NotImplementedException("'excluded' qualified columns are not supported in the RETURNING clause yet");
		}
		return;
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { VerifyNotExcluded((ParsedExpression &)child); });
}

BoundStatement Binder::BindReturning(vector<unique_ptr<ParsedExpression>> returning_list, TableCatalogEntry &table,
                                     const string &alias, idx_t update_table_index,
                                     unique_ptr<LogicalOperator> child_operator, BoundStatement result) {

	vector<LogicalType> types;
	vector<std::string> names;

	auto binder = Binder::CreateBinder(context);

	vector<column_t> bound_columns;
	idx_t column_count = 0;
	for (auto &col : table.GetColumns().Logical()) {
		names.push_back(col.Name());
		types.push_back(col.Type());
		if (!col.Generated()) {
			bound_columns.push_back(column_count);
		}
		column_count++;
	}

	binder->bind_context.AddBaseTable(update_table_index, alias.empty() ? table.name : alias, names, types,
	                                  bound_columns, &table, false);
	ReturningBinder returning_binder(*binder, context);

	vector<unique_ptr<Expression>> projection_expressions;
	LogicalType result_type;
	vector<unique_ptr<ParsedExpression>> new_returning_list;
	binder->ExpandStarExpressions(returning_list, new_returning_list);
	for (auto &returning_expr : new_returning_list) {
		VerifyNotExcluded(*returning_expr);
		auto expr = returning_binder.Bind(returning_expr, &result_type);
		result.names.push_back(expr->GetName());
		result.types.push_back(result_type);
		projection_expressions.push_back(std::move(expr));
	}

	auto projection = make_uniq<LogicalProjection>(GenerateTableIndex(), std::move(projection_expressions));
	projection->AddChild(std::move(child_operator));
	D_ASSERT(result.types.size() == result.names.size());
	result.plan = std::move(projection);
	// If an insert/delete/update statement returns data, there are sometimes issues with streaming results
	// where the data modification doesn't take place until the streamed result is exhausted. Once a row is
	// returned, it should be guaranteed that the row has been inserted.
	// see https://github.com/duckdb/duckdb/issues/8310
	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

} // namespace duckdb




namespace duckdb {

BoundParameterMap::BoundParameterMap(case_insensitive_map_t<BoundParameterData> &parameter_data)
    : parameter_data(parameter_data) {
}

LogicalType BoundParameterMap::GetReturnType(const string &identifier) {
	D_ASSERT(!identifier.empty());
	auto it = parameter_data.find(identifier);
	if (it == parameter_data.end()) {
		return LogicalTypeId::UNKNOWN;
	}
	return it->second.return_type;
}

bound_parameter_map_t *BoundParameterMap::GetParametersPtr() {
	return &parameters;
}

const bound_parameter_map_t &BoundParameterMap::GetParameters() {
	return parameters;
}

const case_insensitive_map_t<BoundParameterData> &BoundParameterMap::GetParameterData() {
	return parameter_data;
}

shared_ptr<BoundParameterData> BoundParameterMap::CreateOrGetData(const string &identifier) {
	auto entry = parameters.find(identifier);
	if (entry == parameters.end()) {
		// no entry yet: create a new one
		auto data = make_shared<BoundParameterData>();
		data->return_type = GetReturnType(identifier);

		CreateNewParameter(identifier, data);
		return data;
	}
	return entry->second;
}

unique_ptr<BoundParameterExpression> BoundParameterMap::BindParameterExpression(ParameterExpression &expr) {
	auto &identifier = expr.identifier;
	auto return_type = GetReturnType(identifier);

	D_ASSERT(!parameter_data.count(identifier));

	// No value has been supplied yet,
	// We return a shared pointer to an object that will get populated wtih a Value later
	// When the BoundParameterExpression get executed, this will be used to get the corresponding value
	auto param_data = CreateOrGetData(identifier);
	auto bound_expr = make_uniq<BoundParameterExpression>(identifier);
	bound_expr->parameter_data = param_data;
	bound_expr->return_type = return_type;
	bound_expr->alias = expr.alias;
	return bound_expr;
}

void BoundParameterMap::CreateNewParameter(const string &id, const shared_ptr<BoundParameterData> &param_data) {
	D_ASSERT(!parameters.count(id));
	parameters.emplace(std::make_pair(id, param_data));
}

} // namespace duckdb


namespace duckdb {

BoundResultModifier::BoundResultModifier(ResultModifierType type) : type(type) {
}

BoundResultModifier::~BoundResultModifier() {
}

BoundOrderByNode::BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression)
    : type(type), null_order(null_order), expression(std::move(expression)) {
}
BoundOrderByNode::BoundOrderByNode(OrderType type, OrderByNullType null_order, unique_ptr<Expression> expression,
                                   unique_ptr<BaseStatistics> stats)
    : type(type), null_order(null_order), expression(std::move(expression)), stats(std::move(stats)) {
}

BoundOrderByNode BoundOrderByNode::Copy() const {
	if (stats) {
		return BoundOrderByNode(type, null_order, expression->Copy(), stats->ToUnique());
	} else {
		return BoundOrderByNode(type, null_order, expression->Copy());
	}
}

bool BoundOrderByNode::Equals(const BoundOrderByNode &other) const {
	if (type != other.type || null_order != other.null_order) {
		return false;
	}
	if (!expression->Equals(*other.expression)) {
		return false;
	}

	return true;
}

string BoundOrderByNode::ToString() const {
	auto str = expression->ToString();
	switch (type) {
	case OrderType::ASCENDING:
		str += " ASC";
		break;
	case OrderType::DESCENDING:
		str += " DESC";
		break;
	default:
		break;
	}

	switch (null_order) {
	case OrderByNullType::NULLS_FIRST:
		str += " NULLS FIRST";
		break;
	case OrderByNullType::NULLS_LAST:
		str += " NULLS LAST";
		break;
	default:
		break;
	}
	return str;
}

unique_ptr<BoundOrderModifier> BoundOrderModifier::Copy() const {
	auto result = make_uniq<BoundOrderModifier>();
	for (auto &order : orders) {
		result->orders.push_back(order.Copy());
	}
	return result;
}

bool BoundOrderModifier::Equals(const BoundOrderModifier &left, const BoundOrderModifier &right) {
	if (left.orders.size() != right.orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.orders.size(); i++) {
		if (!left.orders[i].Equals(right.orders[i])) {
			return false;
		}
	}
	return true;
}

bool BoundOrderModifier::Equals(const unique_ptr<BoundOrderModifier> &left,
                                const unique_ptr<BoundOrderModifier> &right) {
	if (left.get() == right.get()) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return BoundOrderModifier::Equals(*left, *right);
}

BoundLimitModifier::BoundLimitModifier() : BoundResultModifier(ResultModifierType::LIMIT_MODIFIER) {
}

BoundOrderModifier::BoundOrderModifier() : BoundResultModifier(ResultModifierType::ORDER_MODIFIER) {
}

BoundDistinctModifier::BoundDistinctModifier() : BoundResultModifier(ResultModifierType::DISTINCT_MODIFIER) {
}

BoundLimitPercentModifier::BoundLimitPercentModifier()
    : BoundResultModifier(ResultModifierType::LIMIT_PERCENT_MODIFIER) {
}

} // namespace duckdb








namespace duckdb {

BoundAggregateExpression::BoundAggregateExpression(AggregateFunction function, vector<unique_ptr<Expression>> children,
                                                   unique_ptr<Expression> filter, unique_ptr<FunctionData> bind_info,
                                                   AggregateType aggr_type)
    : Expression(ExpressionType::BOUND_AGGREGATE, ExpressionClass::BOUND_AGGREGATE, function.return_type),
      function(std::move(function)), children(std::move(children)), bind_info(std::move(bind_info)),
      aggr_type(aggr_type), filter(std::move(filter)) {
	D_ASSERT(!this->function.name.empty());
}

string BoundAggregateExpression::ToString() const {
	return FunctionExpression::ToString<BoundAggregateExpression, Expression, BoundOrderModifier>(
	    *this, string(), function.name, false, IsDistinct(), filter.get(), order_bys.get());
}

hash_t BoundAggregateExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(result, function.Hash());
	result = CombineHash(result, duckdb::Hash(IsDistinct()));
	return result;
}

bool BoundAggregateExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundAggregateExpression>();
	if (other.aggr_type != aggr_type) {
		return false;
	}
	if (other.function != function) {
		return false;
	}
	if (children.size() != other.children.size()) {
		return false;
	}
	if (!Expression::Equals(other.filter, filter)) {
		return false;
	}
	for (idx_t i = 0; i < children.size(); i++) {
		if (!Expression::Equals(*children[i], *other.children[i])) {
			return false;
		}
	}
	if (!FunctionData::Equals(bind_info.get(), other.bind_info.get())) {
		return false;
	}
	if (!BoundOrderModifier::Equals(order_bys, other.order_bys)) {
		return false;
	}
	return true;
}

bool BoundAggregateExpression::PropagatesNullValues() const {
	return function.null_handling == FunctionNullHandling::SPECIAL_HANDLING ? false
	                                                                        : Expression::PropagatesNullValues();
}

unique_ptr<Expression> BoundAggregateExpression::Copy() {
	vector<unique_ptr<Expression>> new_children;
	new_children.reserve(children.size());
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	auto new_bind_info = bind_info ? bind_info->Copy() : nullptr;
	auto new_filter = filter ? filter->Copy() : nullptr;
	auto copy = make_uniq<BoundAggregateExpression>(function, std::move(new_children), std::move(new_filter),
	                                                std::move(new_bind_info), aggr_type);
	copy->CopyProperties(*this);
	copy->order_bys = order_bys ? order_bys->Copy() : nullptr;
	return std::move(copy);
}

void BoundAggregateExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", children);
	FunctionSerializer::Serialize(serializer, function, bind_info.get());
	serializer.WriteProperty(203, "aggregate_type", aggr_type);
	serializer.WritePropertyWithDefault(204, "filter", filter, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(205, "order_bys", order_bys, unique_ptr<BoundOrderModifier>());
}

unique_ptr<Expression> BoundAggregateExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");
	auto entry = FunctionSerializer::Deserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
	    deserializer, CatalogType::AGGREGATE_FUNCTION_ENTRY, children, std::move(return_type));
	auto aggregate_type = deserializer.ReadProperty<AggregateType>(203, "aggregate_type");
	auto filter = deserializer.ReadPropertyWithDefault<unique_ptr<Expression>>(204, "filter", unique_ptr<Expression>());
	auto result = make_uniq<BoundAggregateExpression>(std::move(entry.first), std::move(children), std::move(filter),
	                                                  std::move(entry.second), aggregate_type);
	deserializer.ReadPropertyWithDefault(205, "order_bys", result->order_bys, unique_ptr<BoundOrderModifier>());
	return std::move(result);
}

} // namespace duckdb



namespace duckdb {

BoundBetweenExpression::BoundBetweenExpression()
    : Expression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BOUND_BETWEEN, LogicalType::BOOLEAN) {
}

BoundBetweenExpression::BoundBetweenExpression(unique_ptr<Expression> input, unique_ptr<Expression> lower,
                                               unique_ptr<Expression> upper, bool lower_inclusive, bool upper_inclusive)
    : Expression(ExpressionType::COMPARE_BETWEEN, ExpressionClass::BOUND_BETWEEN, LogicalType::BOOLEAN),
      input(std::move(input)), lower(std::move(lower)), upper(std::move(upper)), lower_inclusive(lower_inclusive),
      upper_inclusive(upper_inclusive) {
}

string BoundBetweenExpression::ToString() const {
	return BetweenExpression::ToString<BoundBetweenExpression, Expression>(*this);
}

bool BoundBetweenExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundBetweenExpression>();
	if (!Expression::Equals(*input, *other.input)) {
		return false;
	}
	if (!Expression::Equals(*lower, *other.lower)) {
		return false;
	}
	if (!Expression::Equals(*upper, *other.upper)) {
		return false;
	}
	return lower_inclusive == other.lower_inclusive && upper_inclusive == other.upper_inclusive;
}

unique_ptr<Expression> BoundBetweenExpression::Copy() {
	auto copy = make_uniq<BoundBetweenExpression>(input->Copy(), lower->Copy(), upper->Copy(), lower_inclusive,
	                                              upper_inclusive);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb



namespace duckdb {

BoundCaseExpression::BoundCaseExpression(LogicalType type)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, std::move(type)) {
}

BoundCaseExpression::BoundCaseExpression(unique_ptr<Expression> when_expr, unique_ptr<Expression> then_expr,
                                         unique_ptr<Expression> else_expr_p)
    : Expression(ExpressionType::CASE_EXPR, ExpressionClass::BOUND_CASE, then_expr->return_type),
      else_expr(std::move(else_expr_p)) {
	BoundCaseCheck check;
	check.when_expr = std::move(when_expr);
	check.then_expr = std::move(then_expr);
	case_checks.push_back(std::move(check));
}

string BoundCaseExpression::ToString() const {
	return CaseExpression::ToString<BoundCaseExpression, Expression>(*this);
}

bool BoundCaseExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundCaseExpression>();
	if (case_checks.size() != other.case_checks.size()) {
		return false;
	}
	for (idx_t i = 0; i < case_checks.size(); i++) {
		if (!Expression::Equals(*case_checks[i].when_expr, *other.case_checks[i].when_expr)) {
			return false;
		}
		if (!Expression::Equals(*case_checks[i].then_expr, *other.case_checks[i].then_expr)) {
			return false;
		}
	}
	if (!Expression::Equals(*else_expr, *other.else_expr)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCaseExpression::Copy() {
	auto new_case = make_uniq<BoundCaseExpression>(return_type);
	for (auto &check : case_checks) {
		BoundCaseCheck new_check;
		new_check.when_expr = check.when_expr->Copy();
		new_check.then_expr = check.then_expr->Copy();
		new_case->case_checks.push_back(std::move(new_check));
	}
	new_case->else_expr = else_expr->Copy();

	new_case->CopyProperties(*this);
	return std::move(new_case);
}

} // namespace duckdb







namespace duckdb {

static BoundCastInfo BindCastFunction(ClientContext &context, const LogicalType &source, const LogicalType &target) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput input(context);
	return cast_functions.GetCastFunction(source, target, input);
}

BoundCastExpression::BoundCastExpression(unique_ptr<Expression> child_p, LogicalType target_type_p,
                                         BoundCastInfo bound_cast_p, bool try_cast_p)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, std::move(target_type_p)),
      child(std::move(child_p)), try_cast(try_cast_p), bound_cast(std::move(bound_cast_p)) {
}

BoundCastExpression::BoundCastExpression(ClientContext &context, unique_ptr<Expression> child_p,
                                         LogicalType target_type_p)
    : Expression(ExpressionType::OPERATOR_CAST, ExpressionClass::BOUND_CAST, std::move(target_type_p)),
      child(std::move(child_p)), try_cast(false),
      bound_cast(BindCastFunction(context, child->return_type, return_type)) {
}

unique_ptr<Expression> AddCastExpressionInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                                 BoundCastInfo bound_cast, bool try_cast) {
	if (expr->return_type == target_type) {
		return expr;
	}
	auto &expr_type = expr->return_type;
	if (target_type.id() == LogicalTypeId::LIST && expr_type.id() == LogicalTypeId::LIST) {
		auto &target_list = ListType::GetChildType(target_type);
		auto &expr_list = ListType::GetChildType(expr_type);
		if (target_list.id() == LogicalTypeId::ANY || expr_list == target_list) {
			return expr;
		}
	}
	return make_uniq<BoundCastExpression>(std::move(expr), target_type, std::move(bound_cast), try_cast);
}

unique_ptr<Expression> AddCastToTypeInternal(unique_ptr<Expression> expr, const LogicalType &target_type,
                                             CastFunctionSet &cast_functions, GetCastFunctionInput &get_input,
                                             bool try_cast) {
	D_ASSERT(expr);
	if (expr->expression_class == ExpressionClass::BOUND_PARAMETER) {
		auto &parameter = expr->Cast<BoundParameterExpression>();
		if (!target_type.IsValid()) {
			// invalidate the parameter
			parameter.parameter_data->return_type = LogicalType::INVALID;
			parameter.return_type = target_type;
			return expr;
		}
		if (parameter.parameter_data->return_type.id() == LogicalTypeId::INVALID) {
			// we don't know the type of this parameter
			parameter.return_type = target_type;
			return expr;
		}
		if (parameter.parameter_data->return_type.id() == LogicalTypeId::UNKNOWN) {
			// prepared statement parameter cast - but there is no type, convert the type
			parameter.parameter_data->return_type = target_type;
			parameter.return_type = target_type;
			return expr;
		}
		// prepared statement parameter already has a type
		if (parameter.parameter_data->return_type == target_type) {
			// this type! we are done
			parameter.return_type = parameter.parameter_data->return_type;
			return expr;
		}
		// invalidate the type
		parameter.parameter_data->return_type = LogicalType::INVALID;
		parameter.return_type = target_type;
		return expr;
	} else if (expr->expression_class == ExpressionClass::BOUND_DEFAULT) {
		D_ASSERT(target_type.IsValid());
		auto &def = expr->Cast<BoundDefaultExpression>();
		def.return_type = target_type;
	}
	if (!target_type.IsValid()) {
		return expr;
	}

	auto cast_function = cast_functions.GetCastFunction(expr->return_type, target_type, get_input);
	return AddCastExpressionInternal(std::move(expr), target_type, std::move(cast_function), try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddDefaultCastToType(unique_ptr<Expression> expr,
                                                                 const LogicalType &target_type, bool try_cast) {
	CastFunctionSet default_set;
	GetCastFunctionInput get_input;
	return AddCastToTypeInternal(std::move(expr), target_type, default_set, get_input, try_cast);
}

unique_ptr<Expression> BoundCastExpression::AddCastToType(ClientContext &context, unique_ptr<Expression> expr,
                                                          const LogicalType &target_type, bool try_cast) {
	auto &cast_functions = DBConfig::GetConfig(context).GetCastFunctions();
	GetCastFunctionInput get_input(context);
	return AddCastToTypeInternal(std::move(expr), target_type, cast_functions, get_input, try_cast);
}

bool BoundCastExpression::CastIsInvertible(const LogicalType &source_type, const LogicalType &target_type) {
	D_ASSERT(source_type.IsValid() && target_type.IsValid());
	if (source_type.id() == LogicalTypeId::BOOLEAN || target_type.id() == LogicalTypeId::BOOLEAN) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::FLOAT || target_type.id() == LogicalTypeId::FLOAT) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::DOUBLE || target_type.id() == LogicalTypeId::DOUBLE) {
		return false;
	}
	if (source_type.id() == LogicalTypeId::DECIMAL || target_type.id() == LogicalTypeId::DECIMAL) {
		uint8_t source_width, target_width;
		uint8_t source_scale, target_scale;
		// cast to or from decimal
		// cast is only invertible if the cast is strictly widening
		if (!source_type.GetDecimalProperties(source_width, source_scale)) {
			return false;
		}
		if (!target_type.GetDecimalProperties(target_width, target_scale)) {
			return false;
		}
		if (target_scale < source_scale) {
			return false;
		}
		return true;
	}
	if (source_type.id() == LogicalTypeId::TIMESTAMP || source_type.id() == LogicalTypeId::TIMESTAMP_TZ) {
		switch (target_type.id()) {
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIME_TZ:
			return false;
		default:
			break;
		}
	}
	if (source_type.id() == LogicalTypeId::VARCHAR) {
		switch (target_type.id()) {
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIMESTAMP_TZ:
			return true;
		default:
			return false;
		}
	}
	if (target_type.id() == LogicalTypeId::VARCHAR) {
		switch (source_type.id()) {
		case LogicalTypeId::DATE:
		case LogicalTypeId::TIME:
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_NS:
		case LogicalTypeId::TIMESTAMP_MS:
		case LogicalTypeId::TIMESTAMP_SEC:
		case LogicalTypeId::TIME_TZ:
		case LogicalTypeId::TIMESTAMP_TZ:
			return true;
		default:
			return false;
		}
	}
	return true;
}

string BoundCastExpression::ToString() const {
	return (try_cast ? "TRY_CAST(" : "CAST(") + child->GetName() + " AS " + return_type.ToString() + ")";
}

bool BoundCastExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundCastExpression>();
	if (!Expression::Equals(*child, *other.child)) {
		return false;
	}
	if (try_cast != other.try_cast) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundCastExpression::Copy() {
	auto copy = make_uniq<BoundCastExpression>(child->Copy(), return_type, bound_cast.Copy(), try_cast);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb





namespace duckdb {

BoundColumnRefExpression::BoundColumnRefExpression(string alias_p, LogicalType type, ColumnBinding binding, idx_t depth)
    : Expression(ExpressionType::BOUND_COLUMN_REF, ExpressionClass::BOUND_COLUMN_REF, std::move(type)),
      binding(binding), depth(depth) {
	this->alias = std::move(alias_p);
}

BoundColumnRefExpression::BoundColumnRefExpression(LogicalType type, ColumnBinding binding, idx_t depth)
    : BoundColumnRefExpression(string(), std::move(type), binding, depth) {
}

unique_ptr<Expression> BoundColumnRefExpression::Copy() {
	return make_uniq<BoundColumnRefExpression>(alias, return_type, binding, depth);
}

hash_t BoundColumnRefExpression::Hash() const {
	auto result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.column_index));
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.table_index));
	return CombineHash(result, duckdb::Hash<uint64_t>(depth));
}

bool BoundColumnRefExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundColumnRefExpression>();
	return other.binding == binding && other.depth == depth;
}

string BoundColumnRefExpression::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return binding.ToString();
	}
#endif
	return Expression::GetName();
}

string BoundColumnRefExpression::ToString() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return binding.ToString();
	}
#endif
	if (!alias.empty()) {
		return alias;
	}
	return binding.ToString();
}

} // namespace duckdb



namespace duckdb {

BoundComparisonExpression::BoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left,
                                                     unique_ptr<Expression> right)
    : Expression(type, ExpressionClass::BOUND_COMPARISON, LogicalType::BOOLEAN), left(std::move(left)),
      right(std::move(right)) {
}

string BoundComparisonExpression::ToString() const {
	return ComparisonExpression::ToString<BoundComparisonExpression, Expression>(*this);
}

bool BoundComparisonExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundComparisonExpression>();
	if (!Expression::Equals(*left, *other.left)) {
		return false;
	}
	if (!Expression::Equals(*right, *other.right)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundComparisonExpression::Copy() {
	auto copy = make_uniq<BoundComparisonExpression>(type, left->Copy(), right->Copy());
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb




namespace duckdb {

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type)
    : Expression(type, ExpressionClass::BOUND_CONJUNCTION, LogicalType::BOOLEAN) {
}

BoundConjunctionExpression::BoundConjunctionExpression(ExpressionType type, unique_ptr<Expression> left,
                                                       unique_ptr<Expression> right)
    : BoundConjunctionExpression(type) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

string BoundConjunctionExpression::ToString() const {
	return ConjunctionExpression::ToString<BoundConjunctionExpression, Expression>(*this);
}

bool BoundConjunctionExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundConjunctionExpression>();
	return ExpressionUtil::SetEquals(children, other.children);
}

bool BoundConjunctionExpression::PropagatesNullValues() const {
	return false;
}

unique_ptr<Expression> BoundConjunctionExpression::Copy() {
	auto copy = make_uniq<BoundConjunctionExpression>(type);
	for (auto &expr : children) {
		copy->children.push_back(expr->Copy());
	}
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb




namespace duckdb {

BoundConstantExpression::BoundConstantExpression(Value value_p)
    : Expression(ExpressionType::VALUE_CONSTANT, ExpressionClass::BOUND_CONSTANT, value_p.type()),
      value(std::move(value_p)) {
}

string BoundConstantExpression::ToString() const {
	return value.ToSQLString();
}

bool BoundConstantExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundConstantExpression>();
	return value.type() == other.value.type() && !ValueOperations::DistinctFrom(value, other.value);
}

hash_t BoundConstantExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(value.Hash(), result);
}

unique_ptr<Expression> BoundConstantExpression::Copy() {
	auto copy = make_uniq<BoundConstantExpression>(value);
	copy->CopyProperties(*this);
	return std::move(copy);
}

} // namespace duckdb


namespace duckdb {

BoundExpression::BoundExpression(unique_ptr<Expression> expr_p)
    : ParsedExpression(ExpressionType::INVALID, ExpressionClass::BOUND_EXPRESSION), expr(std::move(expr_p)) {
	this->alias = expr->alias;
}

unique_ptr<Expression> &BoundExpression::GetExpression(ParsedExpression &expr) {
	auto &bound_expr = expr.Cast<BoundExpression>();
	if (!bound_expr.expr) {
		throw InternalException("BoundExpression::GetExpression called on empty bound expression");
	}
	return bound_expr.expr;
}

string BoundExpression::ToString() const {
	if (!expr) {
		throw InternalException("ToString(): BoundExpression does not have a child");
	}
	return expr->ToString();
}

bool BoundExpression::Equals(const BaseExpression &other) const {
	return false;
}
hash_t BoundExpression::Hash() const {
	return 0;
}

unique_ptr<ParsedExpression> BoundExpression::Copy() const {
	throw SerializationException("Cannot copy or serialize bound expression");
}

void BoundExpression::Serialize(Serializer &serializer) const {
	throw SerializationException("Cannot copy or serialize bound expression");
}

} // namespace duckdb








namespace duckdb {

BoundFunctionExpression::BoundFunctionExpression(LogicalType return_type, ScalarFunction bound_function,
                                                 vector<unique_ptr<Expression>> arguments,
                                                 unique_ptr<FunctionData> bind_info, bool is_operator)
    : Expression(ExpressionType::BOUND_FUNCTION, ExpressionClass::BOUND_FUNCTION, std::move(return_type)),
      function(std::move(bound_function)), children(std::move(arguments)), bind_info(std::move(bind_info)),
      is_operator(is_operator) {
	D_ASSERT(!function.name.empty());
}

bool BoundFunctionExpression::HasSideEffects() const {
	return function.side_effects == FunctionSideEffects::HAS_SIDE_EFFECTS ? true : Expression::HasSideEffects();
}

bool BoundFunctionExpression::IsFoldable() const {
	// functions with side effects cannot be folded: they have to be executed once for every row
	return function.side_effects == FunctionSideEffects::HAS_SIDE_EFFECTS ? false : Expression::IsFoldable();
}

string BoundFunctionExpression::ToString() const {
	return FunctionExpression::ToString<BoundFunctionExpression, Expression>(*this, string(), function.name,
	                                                                         is_operator);
}
bool BoundFunctionExpression::PropagatesNullValues() const {
	return function.null_handling == FunctionNullHandling::SPECIAL_HANDLING ? false
	                                                                        : Expression::PropagatesNullValues();
}

hash_t BoundFunctionExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, function.Hash());
}

bool BoundFunctionExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundFunctionExpression>();
	if (other.function != function) {
		return false;
	}
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	if (!FunctionData::Equals(bind_info.get(), other.bind_info.get())) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundFunctionExpression::Copy() {
	vector<unique_ptr<Expression>> new_children;
	new_children.reserve(children.size());
	for (auto &child : children) {
		new_children.push_back(child->Copy());
	}
	unique_ptr<FunctionData> new_bind_info = bind_info ? bind_info->Copy() : nullptr;

	auto copy = make_uniq<BoundFunctionExpression>(return_type, function, std::move(new_children),
	                                               std::move(new_bind_info), is_operator);
	copy->CopyProperties(*this);
	return std::move(copy);
}

void BoundFunctionExpression::Verify() const {
	D_ASSERT(!function.name.empty());
}

void BoundFunctionExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", children);
	FunctionSerializer::Serialize(serializer, function, bind_info.get());
	serializer.WriteProperty(202, "is_operator", is_operator);
}

unique_ptr<Expression> BoundFunctionExpression::Deserialize(Deserializer &deserializer) {
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");
	auto entry = FunctionSerializer::Deserialize<ScalarFunction, ScalarFunctionCatalogEntry>(
	    deserializer, CatalogType::SCALAR_FUNCTION_ENTRY, children, return_type);
	auto result = make_uniq<BoundFunctionExpression>(std::move(return_type), std::move(entry.first),
	                                                 std::move(children), std::move(entry.second));
	deserializer.ReadProperty(202, "is_operator", result->is_operator);
	return std::move(result);
}

} // namespace duckdb



namespace duckdb {

BoundLambdaExpression::BoundLambdaExpression(ExpressionType type_p, LogicalType return_type_p,
                                             unique_ptr<Expression> lambda_expr_p, idx_t parameter_count_p)
    : Expression(type_p, ExpressionClass::BOUND_LAMBDA, std::move(return_type_p)),
      lambda_expr(std::move(lambda_expr_p)), parameter_count(parameter_count_p) {
}

string BoundLambdaExpression::ToString() const {
	return lambda_expr->ToString();
}

bool BoundLambdaExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundLambdaExpression>();
	if (!Expression::Equals(*lambda_expr, *other.lambda_expr)) {
		return false;
	}
	if (!Expression::ListEquals(captures, other.captures)) {
		return false;
	}
	if (parameter_count != other.parameter_count) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundLambdaExpression::Copy() {
	auto copy = make_uniq<BoundLambdaExpression>(type, return_type, lambda_expr->Copy(), parameter_count);
	for (auto &capture : captures) {
		copy->captures.push_back(capture->Copy());
	}
	return std::move(copy);
}

} // namespace duckdb





namespace duckdb {

BoundLambdaRefExpression::BoundLambdaRefExpression(string alias_p, LogicalType type, ColumnBinding binding,
                                                   idx_t lambda_index, idx_t depth)
    : Expression(ExpressionType::BOUND_LAMBDA_REF, ExpressionClass::BOUND_LAMBDA_REF, std::move(type)),
      binding(binding), lambda_index(lambda_index), depth(depth) {
	this->alias = std::move(alias_p);
}

BoundLambdaRefExpression::BoundLambdaRefExpression(LogicalType type, ColumnBinding binding, idx_t lambda_index,
                                                   idx_t depth)
    : BoundLambdaRefExpression(string(), std::move(type), binding, lambda_index, depth) {
}

unique_ptr<Expression> BoundLambdaRefExpression::Copy() {
	return make_uniq<BoundLambdaRefExpression>(alias, return_type, binding, lambda_index, depth);
}

hash_t BoundLambdaRefExpression::Hash() const {
	auto result = Expression::Hash();
	result = CombineHash(result, duckdb::Hash<uint64_t>(lambda_index));
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.column_index));
	result = CombineHash(result, duckdb::Hash<uint64_t>(binding.table_index));
	return CombineHash(result, duckdb::Hash<uint64_t>(depth));
}

bool BoundLambdaRefExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundLambdaRefExpression>();
	return other.binding == binding && other.lambda_index == lambda_index && other.depth == depth;
}

string BoundLambdaRefExpression::ToString() const {
	if (!alias.empty()) {
		return alias;
	}
	return "#[" + to_string(binding.table_index) + "." + to_string(binding.column_index) + "." +
	       to_string(lambda_index) + "]";
}

} // namespace duckdb




namespace duckdb {

BoundOperatorExpression::BoundOperatorExpression(ExpressionType type, LogicalType return_type)
    : Expression(type, ExpressionClass::BOUND_OPERATOR, std::move(return_type)) {
}

string BoundOperatorExpression::ToString() const {
	return OperatorExpression::ToString<BoundOperatorExpression, Expression>(*this);
}

bool BoundOperatorExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundOperatorExpression>();
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundOperatorExpression::Copy() {
	auto copy = make_uniq<BoundOperatorExpression>(type, return_type);
	copy->CopyProperties(*this);
	for (auto &child : children) {
		copy->children.push_back(child->Copy());
	}
	return std::move(copy);
}

} // namespace duckdb





namespace duckdb {

BoundParameterExpression::BoundParameterExpression(const string &identifier)
    : Expression(ExpressionType::VALUE_PARAMETER, ExpressionClass::BOUND_PARAMETER,
                 LogicalType(LogicalTypeId::UNKNOWN)),
      identifier(identifier) {
}

BoundParameterExpression::BoundParameterExpression(bound_parameter_map_t &global_parameter_set, string identifier,
                                                   LogicalType return_type,
                                                   shared_ptr<BoundParameterData> parameter_data)
    : Expression(ExpressionType::VALUE_PARAMETER, ExpressionClass::BOUND_PARAMETER, std::move(return_type)),
      identifier(std::move(identifier)) {
	// check if we have already deserialized a parameter with this number
	auto entry = global_parameter_set.find(this->identifier);
	if (entry == global_parameter_set.end()) {
		// we have not - store the entry we deserialized from this parameter expression
		global_parameter_set[this->identifier] = parameter_data;
	} else {
		// we have! use the previously deserialized entry
		parameter_data = entry->second;
	}
	this->parameter_data = std::move(parameter_data);
}

void BoundParameterExpression::Invalidate(Expression &expr) {
	if (expr.type != ExpressionType::VALUE_PARAMETER) {
		throw InternalException("BoundParameterExpression::Invalidate requires a parameter as input");
	}
	auto &bound_parameter = expr.Cast<BoundParameterExpression>();
	bound_parameter.return_type = LogicalTypeId::SQLNULL;
	bound_parameter.parameter_data->return_type = LogicalTypeId::INVALID;
}

void BoundParameterExpression::InvalidateRecursive(Expression &expr) {
	if (expr.type == ExpressionType::VALUE_PARAMETER) {
		Invalidate(expr);
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { InvalidateRecursive(child); });
}

bool BoundParameterExpression::IsScalar() const {
	return true;
}
bool BoundParameterExpression::HasParameter() const {
	return true;
}
bool BoundParameterExpression::IsFoldable() const {
	return false;
}

string BoundParameterExpression::ToString() const {
	return "$" + identifier;
}

bool BoundParameterExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundParameterExpression>();
	return StringUtil::CIEquals(identifier, other.identifier);
}

hash_t BoundParameterExpression::Hash() const {
	hash_t result = Expression::Hash();
	result = CombineHash(duckdb::Hash(identifier.c_str(), identifier.size()), result);
	return result;
}

unique_ptr<Expression> BoundParameterExpression::Copy() {
	auto result = make_uniq<BoundParameterExpression>(identifier);
	result->parameter_data = parameter_data;
	result->return_type = return_type;
	result->CopyProperties(*this);
	return std::move(result);
}

} // namespace duckdb






namespace duckdb {

BoundReferenceExpression::BoundReferenceExpression(string alias, LogicalType type, idx_t index)
    : Expression(ExpressionType::BOUND_REF, ExpressionClass::BOUND_REF, std::move(type)), index(index) {
	this->alias = std::move(alias);
}
BoundReferenceExpression::BoundReferenceExpression(LogicalType type, idx_t index)
    : BoundReferenceExpression(string(), std::move(type), index) {
}

string BoundReferenceExpression::ToString() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return "#" + to_string(index);
	}
#endif
	if (!alias.empty()) {
		return alias;
	}
	return "#" + to_string(index);
}

bool BoundReferenceExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundReferenceExpression>();
	return other.index == index;
}

hash_t BoundReferenceExpression::Hash() const {
	return CombineHash(Expression::Hash(), duckdb::Hash<idx_t>(index));
}

unique_ptr<Expression> BoundReferenceExpression::Copy() {
	return make_uniq<BoundReferenceExpression>(alias, return_type, index);
}

} // namespace duckdb




namespace duckdb {

BoundSubqueryExpression::BoundSubqueryExpression(LogicalType return_type)
    : Expression(ExpressionType::SUBQUERY, ExpressionClass::BOUND_SUBQUERY, std::move(return_type)) {
}

string BoundSubqueryExpression::ToString() const {
	return "SUBQUERY";
}

bool BoundSubqueryExpression::Equals(const BaseExpression &other_p) const {
	// equality between bound subqueries not implemented currently
	return false;
}

unique_ptr<Expression> BoundSubqueryExpression::Copy() {
	throw SerializationException("Cannot copy BoundSubqueryExpression");
}

bool BoundSubqueryExpression::PropagatesNullValues() const {
	// TODO this can be optimized further by checking the actual subquery node
	return false;
}

} // namespace duckdb





namespace duckdb {

BoundUnnestExpression::BoundUnnestExpression(LogicalType return_type)
    : Expression(ExpressionType::BOUND_UNNEST, ExpressionClass::BOUND_UNNEST, std::move(return_type)) {
}

bool BoundUnnestExpression::IsFoldable() const {
	return false;
}

string BoundUnnestExpression::ToString() const {
	return "UNNEST(" + child->ToString() + ")";
}

hash_t BoundUnnestExpression::Hash() const {
	hash_t result = Expression::Hash();
	return CombineHash(result, duckdb::Hash("unnest"));
}

bool BoundUnnestExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundUnnestExpression>();
	if (!Expression::Equals(*child, *other.child)) {
		return false;
	}
	return true;
}

unique_ptr<Expression> BoundUnnestExpression::Copy() {
	auto copy = make_uniq<BoundUnnestExpression>(return_type);
	copy->child = child->Copy();
	return std::move(copy);
}

} // namespace duckdb








namespace duckdb {

BoundWindowExpression::BoundWindowExpression(ExpressionType type, LogicalType return_type,
                                             unique_ptr<AggregateFunction> aggregate,
                                             unique_ptr<FunctionData> bind_info)
    : Expression(type, ExpressionClass::BOUND_WINDOW, std::move(return_type)), aggregate(std::move(aggregate)),
      bind_info(std::move(bind_info)), ignore_nulls(false) {
}

string BoundWindowExpression::ToString() const {
	string function_name = aggregate.get() ? aggregate->name : ExpressionTypeToString(type);
	return WindowExpression::ToString<BoundWindowExpression, Expression, BoundOrderByNode>(*this, string(),
	                                                                                       function_name);
}

bool BoundWindowExpression::Equals(const BaseExpression &other_p) const {
	if (!Expression::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<BoundWindowExpression>();

	if (ignore_nulls != other.ignore_nulls) {
		return false;
	}
	if (start != other.start || end != other.end) {
		return false;
	}
	// check if the child expressions are equivalent
	if (!Expression::ListEquals(children, other.children)) {
		return false;
	}
	// check if the filter expressions are equivalent
	if (!Expression::Equals(filter_expr, other.filter_expr)) {
		return false;
	}

	// check if the framing expressions are equivalent
	if (!Expression::Equals(start_expr, other.start_expr) || !Expression::Equals(end_expr, other.end_expr) ||
	    !Expression::Equals(offset_expr, other.offset_expr) || !Expression::Equals(default_expr, other.default_expr)) {
		return false;
	}

	return KeysAreCompatible(other);
}

bool BoundWindowExpression::KeysAreCompatible(const BoundWindowExpression &other) const {
	// check if the partitions are equivalent
	if (!Expression::ListEquals(partitions, other.partitions)) {
		return false;
	}
	// check if the orderings are equivalent
	if (orders.size() != other.orders.size()) {
		return false;
	}
	for (idx_t i = 0; i < orders.size(); i++) {
		if (!orders[i].Equals(other.orders[i])) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> BoundWindowExpression::Copy() {
	auto new_window = make_uniq<BoundWindowExpression>(type, return_type, nullptr, nullptr);
	new_window->CopyProperties(*this);

	if (aggregate) {
		new_window->aggregate = make_uniq<AggregateFunction>(*aggregate);
	}
	if (bind_info) {
		new_window->bind_info = bind_info->Copy();
	}
	for (auto &child : children) {
		new_window->children.push_back(child->Copy());
	}
	for (auto &e : partitions) {
		new_window->partitions.push_back(e->Copy());
	}
	for (auto &ps : partitions_stats) {
		if (ps) {
			new_window->partitions_stats.push_back(ps->ToUnique());
		} else {
			new_window->partitions_stats.push_back(nullptr);
		}
	}
	for (auto &o : orders) {
		new_window->orders.emplace_back(o.type, o.null_order, o.expression->Copy());
	}

	new_window->filter_expr = filter_expr ? filter_expr->Copy() : nullptr;

	new_window->start = start;
	new_window->end = end;
	new_window->start_expr = start_expr ? start_expr->Copy() : nullptr;
	new_window->end_expr = end_expr ? end_expr->Copy() : nullptr;
	new_window->offset_expr = offset_expr ? offset_expr->Copy() : nullptr;
	new_window->default_expr = default_expr ? default_expr->Copy() : nullptr;
	new_window->ignore_nulls = ignore_nulls;

	return std::move(new_window);
}

void BoundWindowExpression::Serialize(Serializer &serializer) const {
	Expression::Serialize(serializer);
	serializer.WriteProperty(200, "return_type", return_type);
	serializer.WriteProperty(201, "children", children);
	if (type == ExpressionType::WINDOW_AGGREGATE) {
		D_ASSERT(aggregate);
		FunctionSerializer::Serialize(serializer, *aggregate, bind_info.get());
	}
	serializer.WriteProperty(202, "partitions", partitions);
	serializer.WriteProperty(203, "orders", orders);
	serializer.WritePropertyWithDefault(204, "filters", filter_expr, unique_ptr<Expression>());
	serializer.WriteProperty(205, "ignore_nulls", ignore_nulls);
	serializer.WriteProperty(206, "start", start);
	serializer.WriteProperty(207, "end", end);
	serializer.WritePropertyWithDefault(208, "start_expr", start_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(209, "end_expr", end_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(210, "offset_expr", offset_expr, unique_ptr<Expression>());
	serializer.WritePropertyWithDefault(211, "default_expr", default_expr, unique_ptr<Expression>());
}

unique_ptr<Expression> BoundWindowExpression::Deserialize(Deserializer &deserializer) {
	auto expression_type = deserializer.Get<ExpressionType>();
	auto return_type = deserializer.ReadProperty<LogicalType>(200, "return_type");
	auto children = deserializer.ReadProperty<vector<unique_ptr<Expression>>>(201, "children");
	unique_ptr<AggregateFunction> aggregate;
	unique_ptr<FunctionData> bind_info;
	if (expression_type == ExpressionType::WINDOW_AGGREGATE) {
		auto entry = FunctionSerializer::Deserialize<AggregateFunction, AggregateFunctionCatalogEntry>(
		    deserializer, CatalogType::AGGREGATE_FUNCTION_ENTRY, children, return_type);
		aggregate = make_uniq<AggregateFunction>(std::move(entry.first));
		bind_info = std::move(entry.second);
	}
	auto result =
	    make_uniq<BoundWindowExpression>(expression_type, return_type, std::move(aggregate), std::move(bind_info));
	result->children = std::move(children);
	deserializer.ReadProperty(202, "partitions", result->partitions);
	deserializer.ReadProperty(203, "orders", result->orders);
	deserializer.ReadPropertyWithDefault(204, "filters", result->filter_expr, unique_ptr<Expression>());
	deserializer.ReadProperty(205, "ignore_nulls", result->ignore_nulls);
	deserializer.ReadProperty(206, "start", result->start);
	deserializer.ReadProperty(207, "end", result->end);
	deserializer.ReadPropertyWithDefault(208, "start_expr", result->start_expr, unique_ptr<Expression>());
	deserializer.ReadPropertyWithDefault(209, "end_expr", result->end_expr, unique_ptr<Expression>());
	deserializer.ReadPropertyWithDefault(210, "offset_expr", result->offset_expr, unique_ptr<Expression>());
	deserializer.ReadPropertyWithDefault(211, "default_expr", result->default_expr, unique_ptr<Expression>());
	return std::move(result);
}

} // namespace duckdb









namespace duckdb {

Expression::Expression(ExpressionType type, ExpressionClass expression_class, LogicalType return_type)
    : BaseExpression(type, expression_class), return_type(std::move(return_type)) {
}

Expression::~Expression() {
}

bool Expression::IsAggregate() const {
	bool is_aggregate = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) { is_aggregate |= child.IsAggregate(); });
	return is_aggregate;
}

bool Expression::IsWindow() const {
	bool is_window = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) { is_window |= child.IsWindow(); });
	return is_window;
}

bool Expression::IsScalar() const {
	bool is_scalar = true;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (!child.IsScalar()) {
			is_scalar = false;
		}
	});
	return is_scalar;
}

bool Expression::HasSideEffects() const {
	bool has_side_effects = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (child.HasSideEffects()) {
			has_side_effects = true;
		}
	});
	return has_side_effects;
}

bool Expression::PropagatesNullValues() const {
	if (type == ExpressionType::OPERATOR_IS_NULL || type == ExpressionType::OPERATOR_IS_NOT_NULL ||
	    type == ExpressionType::COMPARE_NOT_DISTINCT_FROM || type == ExpressionType::COMPARE_DISTINCT_FROM ||
	    type == ExpressionType::CONJUNCTION_OR || type == ExpressionType::CONJUNCTION_AND ||
	    type == ExpressionType::OPERATOR_COALESCE) {
		return false;
	}
	bool propagate_null_values = true;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (!child.PropagatesNullValues()) {
			propagate_null_values = false;
		}
	});
	return propagate_null_values;
}

bool Expression::IsFoldable() const {
	bool is_foldable = true;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) {
		if (!child.IsFoldable()) {
			is_foldable = false;
		}
	});
	return is_foldable;
}

bool Expression::HasParameter() const {
	bool has_parameter = false;
	ExpressionIterator::EnumerateChildren(*this,
	                                      [&](const Expression &child) { has_parameter |= child.HasParameter(); });
	return has_parameter;
}

bool Expression::HasSubquery() const {
	bool has_subquery = false;
	ExpressionIterator::EnumerateChildren(*this, [&](const Expression &child) { has_subquery |= child.HasSubquery(); });
	return has_subquery;
}

hash_t Expression::Hash() const {
	hash_t hash = duckdb::Hash<uint32_t>((uint32_t)type);
	hash = CombineHash(hash, return_type.Hash());
	ExpressionIterator::EnumerateChildren(*this,
	                                      [&](const Expression &child) { hash = CombineHash(child.Hash(), hash); });
	return hash;
}

bool Expression::Equals(const unique_ptr<Expression> &left, const unique_ptr<Expression> &right) {
	if (left.get() == right.get()) {
		return true;
	}
	if (!left || !right) {
		return false;
	}
	return left->Equals(*right);
}

bool Expression::ListEquals(const vector<unique_ptr<Expression>> &left, const vector<unique_ptr<Expression>> &right) {
	return ExpressionUtil::ListEquals(left, right);
}

} // namespace duckdb




namespace duckdb {

AggregateBinder::AggregateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true) {
}

BindResult AggregateBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		throw ParserException("aggregate function calls cannot contain window function calls");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string AggregateBinder::UnsupportedAggregateMessage() {
	return "aggregate function calls cannot be nested";
}
} // namespace duckdb






namespace duckdb {

AlterBinder::AlterBinder(Binder &binder, ClientContext &context, TableCatalogEntry &table,
                         vector<LogicalIndex> &bound_columns, LogicalType target_type)
    : ExpressionBinder(binder, context), table(table), bound_columns(bound_columns) {
	this->target_type = std::move(target_type);
}

BindResult AlterBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in alter statement");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in alter statement");
	case ExpressionClass::COLUMN_REF:
		return BindColumn(expr.Cast<ColumnRefExpression>());
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string AlterBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in alter statement";
}

BindResult AlterBinder::BindColumn(ColumnRefExpression &colref) {
	if (colref.column_names.size() > 1) {
		return BindQualifiedColumnName(colref, table.name);
	}
	auto idx = table.GetColumnIndex(colref.column_names[0], true);
	if (!idx.IsValid()) {
		throw BinderException("Table does not contain column %s referenced in alter statement!",
		                      colref.column_names[0]);
	}
	if (table.GetColumn(idx).Generated()) {
		throw BinderException("Using generated columns in alter statement not supported");
	}
	bound_columns.push_back(idx);
	return BindResult(make_uniq<BoundReferenceExpression>(table.GetColumn(idx).Type(), bound_columns.size() - 1));
}

} // namespace duckdb













namespace duckdb {

BaseSelectBinder::BaseSelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                                   BoundGroupInformation &info, case_insensitive_map_t<idx_t> alias_map)
    : ExpressionBinder(binder, context), inside_window(false), node(node), info(info), alias_map(std::move(alias_map)) {
}

BaseSelectBinder::BaseSelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                                   BoundGroupInformation &info)
    : BaseSelectBinder(binder, context, node, info, case_insensitive_map_t<idx_t>()) {
}

BindResult BaseSelectBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth);
	case ExpressionClass::DEFAULT:
		return BindResult("SELECT clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindWindow(expr.Cast<WindowExpression>(), depth);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);
	}
}

idx_t BaseSelectBinder::TryBindGroup(ParsedExpression &expr, idx_t depth) {
	// first check the group alias map, if expr is a ColumnRefExpression
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			auto alias_entry = info.alias_map.find(colref.column_names[0]);
			if (alias_entry != info.alias_map.end()) {
				// found entry!
				return alias_entry->second;
			}
		}
	}
	// no alias reference found
	// check the list of group columns for a match
	auto entry = info.map.find(expr);
	if (entry != info.map.end()) {
		return entry->second;
	}
#ifdef DEBUG
	for (auto entry : info.map) {
		D_ASSERT(!entry.first.get().Equals(expr));
		D_ASSERT(!expr.Equals(entry.first.get()));
	}
#endif
	return DConstants::INVALID_INDEX;
}

BindResult BaseSelectBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth) {
	// first try to bind the column reference regularly
	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError()) {
		return result;
	}
	// binding failed
	// check in the alias map
	auto &colref = (expr_ptr.get())->Cast<ColumnRefExpression>();
	if (!colref.IsQualified()) {
		auto alias_entry = alias_map.find(colref.column_names[0]);
		if (alias_entry != alias_map.end()) {
			// found entry!
			auto index = alias_entry->second;
			if (index >= node.select_list.size()) {
				throw BinderException("Column \"%s\" referenced that exists in the SELECT clause - but this column "
				                      "cannot be referenced before it is defined",
				                      colref.column_names[0]);
			}
			if (node.select_list[index]->HasSideEffects()) {
				throw BinderException("Alias \"%s\" referenced in a SELECT clause - but the expression has side "
				                      "effects. This is not yet supported.",
				                      colref.column_names[0]);
			}
			if (node.select_list[index]->HasSubquery()) {
				throw BinderException("Alias \"%s\" referenced in a SELECT clause - but the expression has a subquery."
				                      " This is not yet supported.",
				                      colref.column_names[0]);
			}
			auto result = BindResult(node.select_list[index]->Copy());
			if (result.expression->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &result_expr = result.expression->Cast<BoundColumnRefExpression>();
				result_expr.depth = depth;
			}
			return result;
		}
	}
	// entry was not found in the alias map: return the original error
	return result;
}

BindResult BaseSelectBinder::BindGroupingFunction(OperatorExpression &op, idx_t depth) {
	if (op.children.empty()) {
		throw InternalException("GROUPING requires at least one child");
	}
	if (node.groups.group_expressions.empty()) {
		return BindResult(binder.FormatError(op, "GROUPING statement cannot be used without groups"));
	}
	if (op.children.size() >= 64) {
		return BindResult(binder.FormatError(op, "GROUPING statement cannot have more than 64 groups"));
	}
	vector<idx_t> group_indexes;
	group_indexes.reserve(op.children.size());
	for (auto &child : op.children) {
		ExpressionBinder::QualifyColumnNames(binder, child);
		auto idx = TryBindGroup(*child, depth);
		if (idx == DConstants::INVALID_INDEX) {
			return BindResult(binder.FormatError(
			    op, StringUtil::Format("GROUPING child \"%s\" must be a grouping column", child->GetName())));
		}
		group_indexes.push_back(idx);
	}
	auto col_idx = node.grouping_functions.size();
	node.grouping_functions.push_back(std::move(group_indexes));
	return BindResult(make_uniq<BoundColumnRefExpression>(op.GetName(), LogicalType::BIGINT,
	                                                      ColumnBinding(node.groupings_index, col_idx), depth));
}

BindResult BaseSelectBinder::BindGroup(ParsedExpression &expr, idx_t depth, idx_t group_index) {
	auto it = info.collated_groups.find(group_index);
	if (it != info.collated_groups.end()) {
		// This is an implicitly collated group, so we need to refer to the first() aggregate
		const auto &aggr_index = it->second;
		return BindResult(make_uniq<BoundColumnRefExpression>(expr.GetName(), node.aggregates[aggr_index]->return_type,
		                                                      ColumnBinding(node.aggregate_index, aggr_index), depth));
	} else {
		auto &group = node.groups.group_expressions[group_index];
		return BindResult(make_uniq<BoundColumnRefExpression>(expr.GetName(), group->return_type,
		                                                      ColumnBinding(node.group_index, group_index), depth));
	}
}

bool BaseSelectBinder::QualifyColumnAlias(const ColumnRefExpression &colref) {
	if (!colref.IsQualified()) {
		return alias_map.find(colref.column_names[0]) != alias_map.end() ? true : false;
	}
	return false;
}

} // namespace duckdb






namespace duckdb {

CheckBinder::CheckBinder(Binder &binder, ClientContext &context, string table_p, const ColumnList &columns,
                         physical_index_set_t &bound_columns)
    : ExpressionBinder(binder, context), table(std::move(table_p)), columns(columns), bound_columns(bound_columns) {
	target_type = LogicalType::INTEGER;
}

BindResult CheckBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in check constraints");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in check constraint");
	case ExpressionClass::COLUMN_REF:
		return BindCheckColumn(expr.Cast<ColumnRefExpression>());
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string CheckBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in check constraints";
}

BindResult ExpressionBinder::BindQualifiedColumnName(ColumnRefExpression &colref, const string &table_name) {
	idx_t struct_start = 0;
	if (colref.column_names[0] == table_name) {
		struct_start++;
	}
	auto result = make_uniq_base<ParsedExpression, ColumnRefExpression>(colref.column_names.back());
	for (idx_t i = struct_start; i + 1 < colref.column_names.size(); i++) {
		result = CreateStructExtract(std::move(result), colref.column_names[i]);
	}
	return BindExpression(result, 0);
}

BindResult CheckBinder::BindCheckColumn(ColumnRefExpression &colref) {

	// if this is a lambda parameters, then we temporarily add a BoundLambdaRef,
	// which we capture and remove later
	if (lambda_bindings) {
		for (idx_t i = 0; i < lambda_bindings->size(); i++) {
			if (colref.GetColumnName() == (*lambda_bindings)[i].dummy_name) {
				// FIXME: support lambdas in CHECK constraints
				// FIXME: like so: return (*lambda_bindings)[i].Bind(colref, i, depth);
				throw NotImplementedException("Lambda functions are currently not supported in CHECK constraints.");
			}
		}
	}

	if (colref.column_names.size() > 1) {
		return BindQualifiedColumnName(colref, table);
	}
	if (!columns.ColumnExists(colref.column_names[0])) {
		throw BinderException("Table does not contain column %s referenced in check constraint!",
		                      colref.column_names[0]);
	}
	auto &col = columns.GetColumn(colref.column_names[0]);
	if (col.Generated()) {
		auto bound_expression = col.GeneratedExpression().Copy();
		return BindExpression(bound_expression, 0, false);
	}
	bound_columns.insert(col.Physical());
	D_ASSERT(col.StorageOid() != DConstants::INVALID_INDEX);
	return BindResult(make_uniq<BoundReferenceExpression>(col.Type(), col.StorageOid()));
}

} // namespace duckdb








namespace duckdb {

ColumnAliasBinder::ColumnAliasBinder(BoundSelectNode &node, const case_insensitive_map_t<idx_t> &alias_map)
    : node(node), alias_map(alias_map), visited_select_indexes() {
}

BindResult ColumnAliasBinder::BindAlias(ExpressionBinder &enclosing_binder, ColumnRefExpression &expr, idx_t depth,
                                        bool root_expression) {
	if (expr.IsQualified()) {
		return BindResult(StringUtil::Format("Alias %s cannot be qualified.", expr.ToString()));
	}

	auto alias_entry = alias_map.find(expr.column_names[0]);
	if (alias_entry == alias_map.end()) {
		return BindResult(StringUtil::Format("Alias %s is not found.", expr.ToString()));
	}

	if (visited_select_indexes.find(alias_entry->second) != visited_select_indexes.end()) {
		return BindResult("Cannot resolve self-referential alias");
	}

	// found an alias: bind the alias expression
	auto expression = node.original_expressions[alias_entry->second]->Copy();
	visited_select_indexes.insert(alias_entry->second);

	// since the alias has been found, pass a depth of 0. See Issue 4978 (#16)
	// ColumnAliasBinders are only in Having, Qualify and Where Binders
	auto result = enclosing_binder.BindExpression(expression, 0, root_expression);
	visited_select_indexes.erase(alias_entry->second);
	return result;
}

} // namespace duckdb



namespace duckdb {

ConstantBinder::ConstantBinder(Binder &binder, ClientContext &context, string clause)
    : ExpressionBinder(binder, context), clause(std::move(clause)) {
}

BindResult ConstantBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF: {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			auto value_function = GetSQLValueFunction(colref.GetColumnName());
			if (value_function) {
				expr_ptr = std::move(value_function);
				return BindExpression(expr_ptr, depth, root_expression);
			}
		}
		return BindResult(clause + " cannot contain column names");
	}
	case ExpressionClass::SUBQUERY:
		throw BinderException(clause + " cannot contain subqueries");
	case ExpressionClass::DEFAULT:
		return BindResult(clause + " cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult(clause + " cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string ConstantBinder::UnsupportedAggregateMessage() {
	return clause + " cannot contain aggregates!";
}

} // namespace duckdb








namespace duckdb {

GroupBinder::GroupBinder(Binder &binder, ClientContext &context, SelectNode &node, idx_t group_index,
                         case_insensitive_map_t<idx_t> &alias_map, case_insensitive_map_t<idx_t> &group_alias_map)
    : ExpressionBinder(binder, context), node(node), alias_map(alias_map), group_alias_map(group_alias_map),
      group_index(group_index) {
}

BindResult GroupBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	if (root_expression && depth == 0) {
		switch (expr.expression_class) {
		case ExpressionClass::COLUMN_REF:
			return BindColumnRef(expr.Cast<ColumnRefExpression>());
		case ExpressionClass::CONSTANT:
			return BindConstant(expr.Cast<ConstantExpression>());
		case ExpressionClass::PARAMETER:
			throw ParameterNotAllowedException("Parameter not supported in GROUP BY clause");
		default:
			break;
		}
	}
	switch (expr.expression_class) {
	case ExpressionClass::DEFAULT:
		return BindResult("GROUP BY clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("GROUP BY clause cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string GroupBinder::UnsupportedAggregateMessage() {
	return "GROUP BY clause cannot contain aggregates!";
}

BindResult GroupBinder::BindSelectRef(idx_t entry) {
	if (used_aliases.find(entry) != used_aliases.end()) {
		// the alias has already been bound to before!
		// this happens if we group on the same alias twice
		// e.g. GROUP BY k, k or GROUP BY 1, 1
		// in this case, we can just replace the grouping with a constant since the second grouping has no effect
		// (the constant grouping will be optimized out later)
		return BindResult(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	}
	if (entry >= node.select_list.size()) {
		throw BinderException("GROUP BY term out of range - should be between 1 and %d", (int)node.select_list.size());
	}
	// we replace the root expression, also replace the unbound expression
	unbound_expression = node.select_list[entry]->Copy();
	// move the expression that this refers to here and bind it
	auto select_entry = std::move(node.select_list[entry]);
	auto binding = Bind(select_entry, nullptr, false);
	// now replace the original expression in the select list with a reference to this group
	group_alias_map[to_string(entry)] = bind_index;
	node.select_list[entry] = make_uniq<ColumnRefExpression>(to_string(entry));
	// insert into the set of used aliases
	used_aliases.insert(entry);
	return BindResult(std::move(binding));
}

BindResult GroupBinder::BindConstant(ConstantExpression &constant) {
	// constant as root expression
	if (!constant.value.type().IsIntegral()) {
		// non-integral expression, we just leave the constant here.
		return ExpressionBinder::BindExpression(constant, 0);
	}
	// INTEGER constant: we use the integer as an index into the select list (e.g. GROUP BY 1)
	auto index = (idx_t)constant.value.GetValue<int64_t>();
	return BindSelectRef(index - 1);
}

BindResult GroupBinder::BindColumnRef(ColumnRefExpression &colref) {
	// columns in GROUP BY clauses:
	// FIRST refer to the original tables, and
	// THEN if no match is found refer to aliases in the SELECT list
	// THEN if no match is found, refer to outer queries

	// first try to bind to the base columns (original tables)
	auto result = ExpressionBinder::BindExpression(colref, 0);
	if (result.HasError()) {
		if (colref.IsQualified()) {
			// explicit table name: not an alias reference
			return result;
		}
		// failed to bind the column and the node is the root expression with depth = 0
		// check if refers to an alias in the select clause
		auto alias_name = colref.column_names[0];
		auto entry = alias_map.find(alias_name);
		if (entry == alias_map.end()) {
			// no matching alias found
			return result;
		}
		result = BindResult(BindSelectRef(entry->second));
		if (!result.HasError()) {
			group_alias_map[alias_name] = bind_index;
		}
	}
	return result;
}

} // namespace duckdb








namespace duckdb {

HavingBinder::HavingBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                           case_insensitive_map_t<idx_t> &alias_map, AggregateHandling aggregate_handling)
    : BaseSelectBinder(binder, context, node, info), column_alias_binder(node, alias_map),
      aggregate_handling(aggregate_handling) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult HavingBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = expr_ptr->Cast<ColumnRefExpression>();
	auto alias_result = column_alias_binder.BindAlias(*this, expr, depth, root_expression);
	if (!alias_result.HasError()) {
		if (depth > 0) {
			throw BinderException("Having clause cannot reference alias in correlated subquery");
		}
		return alias_result;
	}
	if (aggregate_handling == AggregateHandling::FORCE_AGGREGATES) {
		if (depth > 0) {
			throw BinderException("Having clause cannot reference column in correlated subquery and group by all");
		}
		auto expr = duckdb::BaseSelectBinder::BindExpression(expr_ptr, depth);
		if (expr.HasError()) {
			return expr;
		}
		auto group_ref = make_uniq<BoundColumnRefExpression>(
		    expr.expression->return_type, ColumnBinding(node.group_index, node.groups.group_expressions.size()));
		node.groups.group_expressions.push_back(std::move(expr.expression));
		return BindResult(std::move(group_ref));
	}
	return BindResult(StringUtil::Format(
	    "column %s must appear in the GROUP BY clause or be used in an aggregate function", expr.ToString()));
}

BindResult HavingBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("HAVING clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return duckdb::BaseSelectBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb







namespace duckdb {

IndexBinder::IndexBinder(Binder &binder, ClientContext &context, optional_ptr<TableCatalogEntry> table,
                         optional_ptr<CreateIndexInfo> info)
    : ExpressionBinder(binder, context), table(table), info(info) {
}

BindResult IndexBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in index expressions");
	case ExpressionClass::SUBQUERY:
		return BindResult("cannot use subquery in index expressions");
	case ExpressionClass::COLUMN_REF: {
		if (table) {
			// WAL replay
			// we assume that the parsed expressions have qualified column names
			// and that the columns exist in the table
			auto &col_ref = expr.Cast<ColumnRefExpression>();
			auto col_idx = table->GetColumnIndex(col_ref.column_names.back());
			auto col_type = table->GetColumn(col_idx).GetType();

			// find the col_idx in the index.column_ids
			auto col_id_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < info->column_ids.size(); i++) {
				if (col_idx.index == info->column_ids[i]) {
					col_id_idx = i;
				}
			}

			if (col_id_idx == DConstants::INVALID_INDEX) {
				throw InternalException("failed to replay CREATE INDEX statement - column id not found");
			}
			return BindResult(
			    make_uniq<BoundColumnRefExpression>(col_ref.GetColumnName(), col_type, ColumnBinding(0, col_id_idx)));
		}
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string IndexBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in index expressions";
}

} // namespace duckdb




namespace duckdb {

InsertBinder::InsertBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult InsertBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("DEFAULT is not allowed here!");
	case ExpressionClass::WINDOW:
		return BindResult("INSERT statement cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string InsertBinder::UnsupportedAggregateMessage() {
	return "INSERT statement cannot contain aggregates!";
}

} // namespace duckdb






namespace duckdb {

LateralBinder::LateralBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

void LateralBinder::ExtractCorrelatedColumns(Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		if (bound_colref.depth > 0) {
			// add the correlated column info
			CorrelatedColumnInfo info(bound_colref);
			if (std::find(correlated_columns.begin(), correlated_columns.end(), info) == correlated_columns.end()) {
				correlated_columns.push_back(std::move(info));
			}
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractCorrelatedColumns(child); });
}

BindResult LateralBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	if (depth == 0) {
		throw InternalException("Lateral binder can only bind correlated columns");
	}
	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (result.HasError()) {
		return result;
	}
	ExtractCorrelatedColumns(*result.expression);
	return result;
}

BindResult LateralBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("LATERAL join cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("LATERAL join cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string LateralBinder::UnsupportedAggregateMessage() {
	return "LATERAL join cannot contain aggregates!";
}

class ExpressionDepthReducer : public LogicalOperatorVisitor {
public:
	explicit ExpressionDepthReducer(const vector<CorrelatedColumnInfo> &correlated) : correlated_columns(correlated) {
	}

protected:
	void ReduceColumnRefDepth(BoundColumnRefExpression &expr) {
		// don't need to reduce this
		if (expr.depth == 0) {
			return;
		}
		for (auto &correlated : correlated_columns) {
			if (correlated.binding == expr.binding) {
				D_ASSERT(expr.depth > 1);
				expr.depth--;
				break;
			}
		}
	}

	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		ReduceColumnRefDepth(expr);
		return nullptr;
	}

	void ReduceExpressionSubquery(BoundSubqueryExpression &expr) {
		for (auto &s_correlated : expr.binder->correlated_columns) {
			for (auto &correlated : correlated_columns) {
				if (correlated == s_correlated) {
					s_correlated.depth--;
					break;
				}
			}
		}
	}

	void ReduceExpressionDepth(Expression &expr) {
		if (expr.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
			ReduceColumnRefDepth(expr.Cast<BoundColumnRefExpression>());
		}
		if (expr.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY) {
			auto &subquery_ref = expr.Cast<BoundSubqueryExpression>();
			ReduceExpressionSubquery(expr.Cast<BoundSubqueryExpression>());
			// Recursively update the depth in the bindings of the children nodes
			ExpressionIterator::EnumerateQueryNodeChildren(
			    *subquery_ref.subquery, [&](Expression &child_expr) { ReduceExpressionDepth(child_expr); });
		}
	}

	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		ReduceExpressionSubquery(expr);
		ExpressionIterator::EnumerateQueryNodeChildren(
		    *expr.subquery, [&](Expression &child_expr) { ReduceExpressionDepth(child_expr); });
		return nullptr;
	}

	const vector<CorrelatedColumnInfo> &correlated_columns;
};

void LateralBinder::ReduceExpressionDepth(LogicalOperator &op, const vector<CorrelatedColumnInfo> &correlated) {
	ExpressionDepthReducer depth_reducer(correlated);
	depth_reducer.VisitOperator(op);
}

} // namespace duckdb












namespace duckdb {

OrderBinder::OrderBinder(vector<Binder *> binders, idx_t projection_index, case_insensitive_map_t<idx_t> &alias_map,
                         parsed_expression_map_t<idx_t> &projection_map, idx_t max_count)
    : binders(std::move(binders)), projection_index(projection_index), max_count(max_count), extra_list(nullptr),
      alias_map(alias_map), projection_map(projection_map) {
}
OrderBinder::OrderBinder(vector<Binder *> binders, idx_t projection_index, SelectNode &node,
                         case_insensitive_map_t<idx_t> &alias_map, parsed_expression_map_t<idx_t> &projection_map)
    : binders(std::move(binders)), projection_index(projection_index), alias_map(alias_map),
      projection_map(projection_map) {
	this->max_count = node.select_list.size();
	this->extra_list = &node.select_list;
}

unique_ptr<Expression> OrderBinder::CreateProjectionReference(ParsedExpression &expr, idx_t index) {
	string alias;
	if (extra_list && index < extra_list->size()) {
		alias = extra_list->at(index)->ToString();
	} else {
		if (!expr.alias.empty()) {
			alias = expr.alias;
		}
	}
	return make_uniq<BoundColumnRefExpression>(std::move(alias), LogicalType::INVALID,
	                                           ColumnBinding(projection_index, index));
}

unique_ptr<Expression> OrderBinder::CreateExtraReference(unique_ptr<ParsedExpression> expr) {
	if (!extra_list) {
		throw InternalException("CreateExtraReference called without extra_list");
	}
	projection_map[*expr] = extra_list->size();
	auto result = CreateProjectionReference(*expr, extra_list->size());
	extra_list->push_back(std::move(expr));
	return result;
}

unique_ptr<Expression> OrderBinder::BindConstant(ParsedExpression &expr, const Value &val) {
	// ORDER BY a constant
	if (!val.type().IsIntegral()) {
		// non-integral expression, we just leave the constant here.
		// ORDER BY <constant> has no effect
		// CONTROVERSIAL: maybe we should throw an error
		return nullptr;
	}
	// INTEGER constant: we use the integer as an index into the select list (e.g. ORDER BY 1)
	auto index = (idx_t)val.GetValue<int64_t>();
	if (index < 1 || index > max_count) {
		throw BinderException("ORDER term out of range - should be between 1 and %lld", (idx_t)max_count);
	}
	return CreateProjectionReference(expr, index - 1);
}

unique_ptr<Expression> OrderBinder::Bind(unique_ptr<ParsedExpression> expr) {
	// in the ORDER BY clause we do not bind children
	// we bind ONLY to the select list
	// if there is no matching entry in the SELECT list already, we add the expression to the SELECT list and refer the
	// new expression the new entry will then be bound later during the binding of the SELECT list we also don't do type
	// resolution here: this only happens after the SELECT list has been bound
	switch (expr->expression_class) {
	case ExpressionClass::CONSTANT: {
		// ORDER BY constant
		// is the ORDER BY expression a constant integer? (e.g. ORDER BY 1)
		auto &constant = expr->Cast<ConstantExpression>();
		return BindConstant(*expr, constant.value);
	}
	case ExpressionClass::COLUMN_REF: {
		// COLUMN REF expression
		// check if we can bind it to an alias in the select list
		auto &colref = expr->Cast<ColumnRefExpression>();
		// if there is an explicit table name we can't bind to an alias
		if (colref.IsQualified()) {
			break;
		}
		// check the alias list
		auto entry = alias_map.find(colref.column_names[0]);
		if (entry != alias_map.end()) {
			// it does! point it to that entry
			return CreateProjectionReference(*expr, entry->second);
		}
		break;
	}
	case ExpressionClass::POSITIONAL_REFERENCE: {
		auto &posref = expr->Cast<PositionalReferenceExpression>();
		if (posref.index < 1 || posref.index > max_count) {
			throw BinderException("ORDER term out of range - should be between 1 and %lld", (idx_t)max_count);
		}
		return CreateProjectionReference(*expr, posref.index - 1);
	}
	case ExpressionClass::PARAMETER: {
		throw ParameterNotAllowedException("Parameter not supported in ORDER BY clause");
	}
	default:
		break;
	}
	// general case
	// first bind the table names of this entry
	for (auto &binder : binders) {
		ExpressionBinder::QualifyColumnNames(*binder, expr);
	}
	// first check if the ORDER BY clause already points to an entry in the projection list
	auto entry = projection_map.find(*expr);
	if (entry != projection_map.end()) {
		if (entry->second == DConstants::INVALID_INDEX) {
			throw BinderException("Ambiguous reference to column");
		}
		// there is a matching entry in the projection list
		// just point to that entry
		return CreateProjectionReference(*expr, entry->second);
	}
	if (!extra_list) {
		// no extra list specified: we cannot push an extra ORDER BY clause
		throw BinderException("Could not ORDER BY column \"%s\": add the expression/function to every SELECT, or move "
		                      "the UNION into a FROM clause.",
		                      expr->ToString());
	}
	// otherwise we need to push the ORDER BY entry into the select list
	return CreateExtraReference(std::move(expr));
}

} // namespace duckdb









namespace duckdb {

QualifyBinder::QualifyBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                             case_insensitive_map_t<idx_t> &alias_map)
    : BaseSelectBinder(binder, context, node, info), column_alias_binder(node, alias_map) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult QualifyBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = expr_ptr->Cast<ColumnRefExpression>();
	auto result = duckdb::BaseSelectBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError()) {
		return result;
	}

	auto alias_result = column_alias_binder.BindAlias(*this, expr, depth, root_expression);
	if (!alias_result.HasError()) {
		return alias_result;
	}

	return BindResult(StringUtil::Format("Referenced column %s not found in FROM clause and can't find in alias map.",
	                                     expr.ToString()));
}

BindResult QualifyBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr, depth);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindWindow(expr.Cast<WindowExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return duckdb::BaseSelectBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb


namespace duckdb {

RelationBinder::RelationBinder(Binder &binder, ClientContext &context, string op)
    : ExpressionBinder(binder, context), op(std::move(op)) {
}

BindResult RelationBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::AGGREGATE:
		return BindResult("aggregate functions are not allowed in " + op);
	case ExpressionClass::DEFAULT:
		return BindResult(op + " cannot contain DEFAULT clause");
	case ExpressionClass::SUBQUERY:
		return BindResult("subqueries are not allowed in " + op);
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in " + op);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string RelationBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in " + op;
}

} // namespace duckdb




namespace duckdb {

ReturningBinder::ReturningBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult ReturningBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::SUBQUERY:
		return BindResult("SUBQUERY is not supported in returning statements");
	case ExpressionClass::BOUND_SUBQUERY:
		return BindResult("BOUND SUBQUERY is not supported in returning statements");
	case ExpressionClass::COLUMN_REF:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

} // namespace duckdb


namespace duckdb {

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                           case_insensitive_map_t<idx_t> alias_map)
    : BaseSelectBinder(binder, context, node, info, std::move(alias_map)) {
}

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : SelectBinder(binder, context, node, info, case_insensitive_map_t<idx_t>()) {
}

} // namespace duckdb





namespace duckdb {

TableFunctionBinder::TableFunctionBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult TableFunctionBinder::BindColumnReference(ColumnRefExpression &expr, idx_t depth, bool root_expression) {

	// if this is a lambda parameters, then we temporarily add a BoundLambdaRef,
	// which we capture and remove later
	if (lambda_bindings) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		for (idx_t i = 0; i < lambda_bindings->size(); i++) {
			if (colref.GetColumnName() == (*lambda_bindings)[i].dummy_name) {
				return (*lambda_bindings)[i].Bind(colref, i, depth);
			}
		}
	}
	auto value_function = ExpressionBinder::GetSQLValueFunction(expr.GetColumnName());
	if (value_function) {
		return BindExpression(value_function, depth, root_expression);
	}

	auto result_name = StringUtil::Join(expr.column_names, ".");
	return BindResult(make_uniq<BoundConstantExpression>(Value(result_name)));
}

BindResult TableFunctionBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                               bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::COLUMN_REF:
		return BindColumnReference(expr.Cast<ColumnRefExpression>(), depth, root_expression);
	case ExpressionClass::SUBQUERY:
		throw BinderException("Table function cannot contain subqueries");
	case ExpressionClass::DEFAULT:
		return BindResult("Table function cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("Table function cannot contain window functions!");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string TableFunctionBinder::UnsupportedAggregateMessage() {
	return "Table function cannot contain aggregates!";
}

} // namespace duckdb


namespace duckdb {

UpdateBinder::UpdateBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context) {
}

BindResult UpdateBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.expression_class) {
	case ExpressionClass::WINDOW:
		return BindResult("window functions are not allowed in UPDATE");
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string UpdateBinder::UnsupportedAggregateMessage() {
	return "aggregate functions are not allowed in UPDATE";
}

} // namespace duckdb




namespace duckdb {

WhereBinder::WhereBinder(Binder &binder, ClientContext &context, optional_ptr<ColumnAliasBinder> column_alias_binder)
    : ExpressionBinder(binder, context), column_alias_binder(column_alias_binder) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult WhereBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = expr_ptr->Cast<ColumnRefExpression>();
	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError() || !column_alias_binder) {
		return result;
	}

	BindResult alias_result = column_alias_binder->BindAlias(*this, expr, depth, root_expression);
	// This code path cannot be exercised at thispoint. #1547 might change that.
	if (!alias_result.HasError()) {
		return alias_result;
	}

	return result;
}

BindResult WhereBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("WHERE clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("WHERE clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string WhereBinder::UnsupportedAggregateMessage() {
	return "WHERE clause cannot contain aggregates!";
}

} // namespace duckdb








namespace duckdb {

ExpressionBinder::ExpressionBinder(Binder &binder, ClientContext &context, bool replace_binder)
    : binder(binder), context(context) {
	InitializeStackCheck();
	if (replace_binder) {
		stored_binder = &binder.GetActiveBinder();
		binder.SetActiveBinder(*this);
	} else {
		binder.PushExpressionBinder(*this);
	}
}

ExpressionBinder::~ExpressionBinder() {
	if (binder.HasActiveBinder()) {
		if (stored_binder) {
			binder.SetActiveBinder(*stored_binder);
		} else {
			binder.PopExpressionBinder();
		}
	}
}

void ExpressionBinder::InitializeStackCheck() {
	if (binder.HasActiveBinder()) {
		stack_depth = binder.GetActiveBinder().stack_depth;
	} else {
		stack_depth = 0;
	}
}

StackChecker<ExpressionBinder> ExpressionBinder::StackCheck(const ParsedExpression &expr, idx_t extra_stack) {
	D_ASSERT(stack_depth != DConstants::INVALID_INDEX);
	if (stack_depth + extra_stack >= MAXIMUM_STACK_DEPTH) {
		throw BinderException("Maximum recursion depth exceeded (Maximum: %llu) while binding \"%s\"",
		                      MAXIMUM_STACK_DEPTH, expr.ToString());
	}
	return StackChecker<ExpressionBinder>(*this, extra_stack);
}

BindResult ExpressionBinder::BindExpression(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	auto stack_checker = StackCheck(*expr);

	auto &expr_ref = *expr;
	switch (expr_ref.expression_class) {
	case ExpressionClass::BETWEEN:
		return BindExpression(expr_ref.Cast<BetweenExpression>(), depth);
	case ExpressionClass::CASE:
		return BindExpression(expr_ref.Cast<CaseExpression>(), depth);
	case ExpressionClass::CAST:
		return BindExpression(expr_ref.Cast<CastExpression>(), depth);
	case ExpressionClass::COLLATE:
		return BindExpression(expr_ref.Cast<CollateExpression>(), depth);
	case ExpressionClass::COLUMN_REF:
		return BindExpression(expr_ref.Cast<ColumnRefExpression>(), depth);
	case ExpressionClass::COMPARISON:
		return BindExpression(expr_ref.Cast<ComparisonExpression>(), depth);
	case ExpressionClass::CONJUNCTION:
		return BindExpression(expr_ref.Cast<ConjunctionExpression>(), depth);
	case ExpressionClass::CONSTANT:
		return BindExpression(expr_ref.Cast<ConstantExpression>(), depth);
	case ExpressionClass::FUNCTION: {
		auto &function = expr_ref.Cast<FunctionExpression>();
		if (function.function_name == "unnest" || function.function_name == "unlist") {
			// special case, not in catalog
			return BindUnnest(function, depth, root_expression);
		}
		// binding function expression has extra parameter needed for macro's
		return BindExpression(function, depth, expr);
	}
	case ExpressionClass::LAMBDA:
		return BindExpression(expr_ref.Cast<LambdaExpression>(), depth, false, LogicalTypeId::INVALID);
	case ExpressionClass::OPERATOR:
		return BindExpression(expr_ref.Cast<OperatorExpression>(), depth);
	case ExpressionClass::SUBQUERY:
		return BindExpression(expr_ref.Cast<SubqueryExpression>(), depth);
	case ExpressionClass::PARAMETER:
		return BindExpression(expr_ref.Cast<ParameterExpression>(), depth);
	case ExpressionClass::POSITIONAL_REFERENCE: {
		return BindPositionalReference(expr, depth, root_expression);
	}
	case ExpressionClass::STAR:
		return BindResult(binder.FormatError(expr_ref, "STAR expression is not supported here"));
	default:
		throw NotImplementedException("Unimplemented expression class");
	}
}

bool ExpressionBinder::BindCorrelatedColumns(unique_ptr<ParsedExpression> &expr) {
	// try to bind in one of the outer queries, if the binding error occurred in a subquery
	auto &active_binders = binder.GetActiveBinders();
	// make a copy of the set of binders, so we can restore it later
	auto binders = active_binders;

	// we already failed with the current binder
	active_binders.pop_back();
	idx_t depth = 1;
	bool success = false;

	while (!active_binders.empty()) {
		auto &next_binder = active_binders.back().get();
		ExpressionBinder::QualifyColumnNames(next_binder.binder, expr);
		auto bind_result = next_binder.Bind(expr, depth);
		if (bind_result.empty()) {
			success = true;
			break;
		}
		depth++;
		active_binders.pop_back();
	}
	active_binders = binders;
	return success;
}

void ExpressionBinder::BindChild(unique_ptr<ParsedExpression> &expr, idx_t depth, string &error) {
	if (expr) {
		string bind_error = Bind(expr, depth);
		if (error.empty()) {
			error = bind_error;
		}
	}
}

void ExpressionBinder::ExtractCorrelatedExpressions(Binder &binder, Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bound_colref = expr.Cast<BoundColumnRefExpression>();
		if (bound_colref.depth > 0) {
			binder.AddCorrelatedColumn(CorrelatedColumnInfo(bound_colref));
		}
	}
	ExpressionIterator::EnumerateChildren(expr,
	                                      [&](Expression &child) { ExtractCorrelatedExpressions(binder, child); });
}

bool ExpressionBinder::ContainsType(const LogicalType &type, LogicalTypeId target) {
	if (type.id() == target) {
		return true;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto child_count = StructType::GetChildCount(type);
		for (idx_t i = 0; i < child_count; i++) {
			if (ContainsType(StructType::GetChildType(type, i), target)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::UNION: {
		auto member_count = UnionType::GetMemberCount(type);
		for (idx_t i = 0; i < member_count; i++) {
			if (ContainsType(UnionType::GetMemberType(type, i), target)) {
				return true;
			}
		}
		return false;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return ContainsType(ListType::GetChildType(type), target);
	default:
		return false;
	}
}

LogicalType ExpressionBinder::ExchangeType(const LogicalType &type, LogicalTypeId target, LogicalType new_type) {
	if (type.id() == target) {
		return new_type;
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		// we make a copy of the child types of the struct here
		auto child_types = StructType::GetChildTypes(type);
		for (auto &child_type : child_types) {
			child_type.second = ExchangeType(child_type.second, target, new_type);
		}
		return LogicalType::STRUCT(child_types);
	}
	case LogicalTypeId::UNION: {
		auto member_types = UnionType::CopyMemberTypes(type);
		for (auto &member_type : member_types) {
			member_type.second = ExchangeType(member_type.second, target, new_type);
		}
		return LogicalType::UNION(std::move(member_types));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(ExchangeType(ListType::GetChildType(type), target, new_type));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(ExchangeType(ListType::GetChildType(type), target, new_type));
	default:
		return type;
	}
}

bool ExpressionBinder::ContainsNullType(const LogicalType &type) {
	return ContainsType(type, LogicalTypeId::SQLNULL);
}

LogicalType ExpressionBinder::ExchangeNullType(const LogicalType &type) {
	return ExchangeType(type, LogicalTypeId::SQLNULL, LogicalType::INTEGER);
}

unique_ptr<Expression> ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, optional_ptr<LogicalType> result_type,
                                              bool root_expression) {
	// bind the main expression
	auto error_msg = Bind(expr, 0, root_expression);
	if (!error_msg.empty()) {
		// failed to bind: try to bind correlated columns in the expression (if any)
		bool success = BindCorrelatedColumns(expr);
		if (!success) {
			throw BinderException(error_msg);
		}
		auto &bound_expr = expr->Cast<BoundExpression>();
		ExtractCorrelatedExpressions(binder, *bound_expr.expr);
	}
	auto &bound_expr = expr->Cast<BoundExpression>();
	unique_ptr<Expression> result = std::move(bound_expr.expr);
	if (target_type.id() != LogicalTypeId::INVALID) {
		// the binder has a specific target type: add a cast to that type
		result = BoundCastExpression::AddCastToType(context, std::move(result), target_type);
	} else {
		if (!binder.can_contain_nulls) {
			// SQL NULL type is only used internally in the binder
			// cast to INTEGER if we encounter it outside of the binder
			if (ContainsNullType(result->return_type)) {
				auto exchanged_type = ExchangeNullType(result->return_type);
				result = BoundCastExpression::AddCastToType(context, std::move(result), exchanged_type);
			}
		}
		if (result->return_type.id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}
	if (result_type) {
		*result_type = result->return_type;
	}
	return result;
}

string ExpressionBinder::Bind(unique_ptr<ParsedExpression> &expr, idx_t depth, bool root_expression) {
	// bind the node, but only if it has not been bound yet
	auto &expression = *expr;
	auto alias = expression.alias;
	if (expression.GetExpressionClass() == ExpressionClass::BOUND_EXPRESSION) {
		// already bound, don't bind it again
		return string();
	}
	// bind the expression
	BindResult result = BindExpression(expr, depth, root_expression);
	if (result.HasError()) {
		return result.error;
	}
	// successfully bound: replace the node with a BoundExpression
	expr = make_uniq<BoundExpression>(std::move(result.expression));
	auto &be = expr->Cast<BoundExpression>();
	be.alias = alias;
	if (!alias.empty()) {
		be.expr->alias = alias;
	}
	return string();
}

} // namespace duckdb










namespace duckdb {

void ExpressionIterator::EnumerateChildren(const Expression &expr,
                                           const std::function<void(const Expression &child)> &callback) {
	EnumerateChildren((Expression &)expr, [&](unique_ptr<Expression> &child) { callback(*child); });
}

void ExpressionIterator::EnumerateChildren(Expression &expr, const std::function<void(Expression &child)> &callback) {
	EnumerateChildren(expr, [&](unique_ptr<Expression> &child) { callback(*child); });
}

void ExpressionIterator::EnumerateChildren(Expression &expr,
                                           const std::function<void(unique_ptr<Expression> &child)> &callback) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_AGGREGATE: {
		auto &aggr_expr = expr.Cast<BoundAggregateExpression>();
		for (auto &child : aggr_expr.children) {
			callback(child);
		}
		if (aggr_expr.filter) {
			callback(aggr_expr.filter);
		}
		if (aggr_expr.order_bys) {
			for (auto &order : aggr_expr.order_bys->orders) {
				callback(order.expression);
			}
		}
		break;
	}
	case ExpressionClass::BOUND_BETWEEN: {
		auto &between_expr = expr.Cast<BoundBetweenExpression>();
		callback(between_expr.input);
		callback(between_expr.lower);
		callback(between_expr.upper);
		break;
	}
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		for (auto &case_check : case_expr.case_checks) {
			callback(case_check.when_expr);
			callback(case_check.then_expr);
		}
		callback(case_expr.else_expr);
		break;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast_expr = expr.Cast<BoundCastExpression>();
		callback(cast_expr.child);
		break;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp_expr = expr.Cast<BoundComparisonExpression>();
		callback(comp_expr.left);
		callback(comp_expr.right);
		break;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj_expr = expr.Cast<BoundConjunctionExpression>();
		for (auto &child : conj_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &func_expr = expr.Cast<BoundFunctionExpression>();
		for (auto &child : func_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op_expr = expr.Cast<BoundOperatorExpression>();
		for (auto &child : op_expr.children) {
			callback(child);
		}
		break;
	}
	case ExpressionClass::BOUND_SUBQUERY: {
		auto &subquery_expr = expr.Cast<BoundSubqueryExpression>();
		if (subquery_expr.child) {
			callback(subquery_expr.child);
		}
		break;
	}
	case ExpressionClass::BOUND_WINDOW: {
		auto &window_expr = expr.Cast<BoundWindowExpression>();
		for (auto &partition : window_expr.partitions) {
			callback(partition);
		}
		for (auto &order : window_expr.orders) {
			callback(order.expression);
		}
		for (auto &child : window_expr.children) {
			callback(child);
		}
		if (window_expr.filter_expr) {
			callback(window_expr.filter_expr);
		}
		if (window_expr.start_expr) {
			callback(window_expr.start_expr);
		}
		if (window_expr.end_expr) {
			callback(window_expr.end_expr);
		}
		if (window_expr.offset_expr) {
			callback(window_expr.offset_expr);
		}
		if (window_expr.default_expr) {
			callback(window_expr.default_expr);
		}
		break;
	}
	case ExpressionClass::BOUND_UNNEST: {
		auto &unnest_expr = expr.Cast<BoundUnnestExpression>();
		callback(unnest_expr.child);
		break;
	}
	case ExpressionClass::BOUND_COLUMN_REF:
	case ExpressionClass::BOUND_LAMBDA_REF:
	case ExpressionClass::BOUND_CONSTANT:
	case ExpressionClass::BOUND_DEFAULT:
	case ExpressionClass::BOUND_PARAMETER:
	case ExpressionClass::BOUND_REF:
		// these node types have no children
		break;
	default:
		throw InternalException("ExpressionIterator used on unbound expression");
	}
}

void ExpressionIterator::EnumerateExpression(unique_ptr<Expression> &expr,
                                             const std::function<void(Expression &child)> &callback) {
	if (!expr) {
		return;
	}
	callback(*expr);
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { EnumerateExpression(child, callback); });
}

void ExpressionIterator::EnumerateTableRefChildren(BoundTableRef &ref,
                                                   const std::function<void(Expression &child)> &callback) {
	switch (ref.type) {
	case TableReferenceType::EXPRESSION_LIST: {
		auto &bound_expr_list = ref.Cast<BoundExpressionListRef>();
		for (auto &expr_list : bound_expr_list.values) {
			for (auto &expr : expr_list) {
				EnumerateExpression(expr, callback);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &bound_join = ref.Cast<BoundJoinRef>();
		if (bound_join.condition) {
			EnumerateExpression(bound_join.condition, callback);
		}
		EnumerateTableRefChildren(*bound_join.left, callback);
		EnumerateTableRefChildren(*bound_join.right, callback);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &bound_subquery = ref.Cast<BoundSubqueryRef>();
		EnumerateQueryNodeChildren(*bound_subquery.subquery, callback);
		break;
	}
	case TableReferenceType::TABLE_FUNCTION:
	case TableReferenceType::EMPTY:
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::CTE:
		break;
	default:
		throw NotImplementedException("Unimplemented table reference type in ExpressionIterator");
	}
}

void ExpressionIterator::EnumerateQueryNodeChildren(BoundQueryNode &node,
                                                    const std::function<void(Expression &child)> &callback) {
	switch (node.type) {
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &bound_setop = node.Cast<BoundSetOperationNode>();
		EnumerateQueryNodeChildren(*bound_setop.left, callback);
		EnumerateQueryNodeChildren(*bound_setop.right, callback);
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &cte_node = node.Cast<BoundRecursiveCTENode>();
		EnumerateQueryNodeChildren(*cte_node.left, callback);
		EnumerateQueryNodeChildren(*cte_node.right, callback);
		break;
	}
	case QueryNodeType::CTE_NODE: {
		auto &cte_node = node.Cast<BoundCTENode>();
		EnumerateQueryNodeChildren(*cte_node.child, callback);
		break;
	}
	case QueryNodeType::SELECT_NODE: {
		auto &bound_select = node.Cast<BoundSelectNode>();
		for (auto &expr : bound_select.select_list) {
			EnumerateExpression(expr, callback);
		}
		EnumerateExpression(bound_select.where_clause, callback);
		for (auto &expr : bound_select.groups.group_expressions) {
			EnumerateExpression(expr, callback);
		}
		EnumerateExpression(bound_select.having, callback);
		for (auto &expr : bound_select.aggregates) {
			EnumerateExpression(expr, callback);
		}
		for (auto &entry : bound_select.unnests) {
			for (auto &expr : entry.second.expressions) {
				EnumerateExpression(expr, callback);
			}
		}
		for (auto &expr : bound_select.windows) {
			EnumerateExpression(expr, callback);
		}
		if (bound_select.from_table) {
			EnumerateTableRefChildren(*bound_select.from_table, callback);
		}
		break;
	}
	default:
		throw NotImplementedException("Unimplemented query node in ExpressionIterator");
	}
	for (idx_t i = 0; i < node.modifiers.size(); i++) {
		switch (node.modifiers[i]->type) {
		case ResultModifierType::DISTINCT_MODIFIER:
			for (auto &expr : node.modifiers[i]->Cast<BoundDistinctModifier>().target_distincts) {
				EnumerateExpression(expr, callback);
			}
			break;
		case ResultModifierType::ORDER_MODIFIER:
			for (auto &order : node.modifiers[i]->Cast<BoundOrderModifier>().orders) {
				EnumerateExpression(order.expression, callback);
			}
			break;
		default:
			break;
		}
	}
}

} // namespace duckdb


namespace duckdb {

ConjunctionOrFilter::ConjunctionOrFilter() : ConjunctionFilter(TableFilterType::CONJUNCTION_OR) {
}

FilterPropagateResult ConjunctionOrFilter::CheckStatistics(BaseStatistics &stats) {
	// the OR filter is true if ANY of the children is true
	D_ASSERT(!child_filters.empty());
	for (auto &filter : child_filters) {
		auto prune_result = filter->CheckStatistics(stats);
		if (prune_result == FilterPropagateResult::NO_PRUNING_POSSIBLE) {
			return FilterPropagateResult::NO_PRUNING_POSSIBLE;
		} else if (prune_result == FilterPropagateResult::FILTER_ALWAYS_TRUE) {
			return FilterPropagateResult::FILTER_ALWAYS_TRUE;
		}
	}
	return FilterPropagateResult::FILTER_ALWAYS_FALSE;
}

string ConjunctionOrFilter::ToString(const string &column_name) {
	string result;
	for (idx_t i = 0; i < child_filters.size(); i++) {
		if (i > 0) {
			result += " OR ";
		}
		result += child_filters[i]->ToString(column_name);
	}
	return result;
}

bool ConjunctionOrFilter::Equals(const TableFilter &other_p) const {
	if (!ConjunctionFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ConjunctionOrFilter>();
	if (other.child_filters.size() != child_filters.size()) {
		return false;
	}
	for (idx_t i = 0; i < other.child_filters.size(); i++) {
		if (!child_filters[i]->Equals(*other.child_filters[i])) {
			return false;
		}
	}
	return true;
}

ConjunctionAndFilter::ConjunctionAndFilter() : ConjunctionFilter(TableFilterType::CONJUNCTION_AND) {
}

FilterPropagateResult ConjunctionAndFilter::CheckStatistics(BaseStatistics &stats) {
	// the AND filter is true if ALL of the children is true
	D_ASSERT(!child_filters.empty());
	auto result = FilterPropagateResult::FILTER_ALWAYS_TRUE;
	for (auto &filter : child_filters) {
		auto prune_result = filter->CheckStatistics(stats);
		if (prune_result == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			return FilterPropagateResult::FILTER_ALWAYS_FALSE;
		} else if (prune_result != result) {
			result = FilterPropagateResult::NO_PRUNING_POSSIBLE;
		}
	}
	return result;
}

string ConjunctionAndFilter::ToString(const string &column_name) {
	string result;
	for (idx_t i = 0; i < child_filters.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += child_filters[i]->ToString(column_name);
	}
	return result;
}

bool ConjunctionAndFilter::Equals(const TableFilter &other_p) const {
	if (!ConjunctionFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ConjunctionAndFilter>();
	if (other.child_filters.size() != child_filters.size()) {
		return false;
	}
	for (idx_t i = 0; i < other.child_filters.size(); i++) {
		if (!child_filters[i]->Equals(*other.child_filters[i])) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb



namespace duckdb {

ConstantFilter::ConstantFilter(ExpressionType comparison_type_p, Value constant_p)
    : TableFilter(TableFilterType::CONSTANT_COMPARISON), comparison_type(comparison_type_p),
      constant(std::move(constant_p)) {
}

FilterPropagateResult ConstantFilter::CheckStatistics(BaseStatistics &stats) {
	D_ASSERT(constant.type().id() == stats.GetType().id());
	switch (constant.type().InternalType()) {
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return NumericStats::CheckZonemap(stats, comparison_type, constant);
	case PhysicalType::VARCHAR:
		return StringStats::CheckZonemap(stats, comparison_type, StringValue::Get(constant));
	default:
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
}

string ConstantFilter::ToString(const string &column_name) {
	return column_name + ExpressionTypeToOperator(comparison_type) + constant.ToString();
}

bool ConstantFilter::Equals(const TableFilter &other_p) const {
	if (!TableFilter::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<ConstantFilter>();
	return other.comparison_type == comparison_type && other.constant == constant;
}

} // namespace duckdb



namespace duckdb {

IsNullFilter::IsNullFilter() : TableFilter(TableFilterType::IS_NULL) {
}

FilterPropagateResult IsNullFilter::CheckStatistics(BaseStatistics &stats) {
	if (!stats.CanHaveNull()) {
		// no null values are possible: always false
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!stats.CanHaveNoNull()) {
		// no non-null values are possible: always true
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string IsNullFilter::ToString(const string &column_name) {
	return column_name + "IS NULL";
}

IsNotNullFilter::IsNotNullFilter() : TableFilter(TableFilterType::IS_NOT_NULL) {
}

FilterPropagateResult IsNotNullFilter::CheckStatistics(BaseStatistics &stats) {
	if (!stats.CanHaveNoNull()) {
		// no non-null values are possible: always false
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	if (!stats.CanHaveNull()) {
		// no null values are possible: always true
		return FilterPropagateResult::FILTER_ALWAYS_TRUE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

string IsNotNullFilter::ToString(const string &column_name) {
	return column_name + " IS NOT NULL";
}

} // namespace duckdb








namespace duckdb {

unique_ptr<Expression> JoinCondition::CreateExpression(JoinCondition cond) {
	auto bound_comparison =
	    make_uniq<BoundComparisonExpression>(cond.comparison, std::move(cond.left), std::move(cond.right));
	return std::move(bound_comparison);
}

unique_ptr<Expression> JoinCondition::CreateExpression(vector<JoinCondition> conditions) {
	unique_ptr<Expression> result;
	for (auto &cond : conditions) {
		auto expr = CreateExpression(std::move(cond));
		if (!result) {
			result = std::move(expr);
		} else {
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(expr),
			                                                  std::move(result));
			result = std::move(conj);
		}
	}
	return result;
}

JoinSide JoinSide::CombineJoinSide(JoinSide left, JoinSide right) {
	if (left == JoinSide::NONE) {
		return right;
	}
	if (right == JoinSide::NONE) {
		return left;
	}
	if (left != right) {
		return JoinSide::BOTH;
	}
	return left;
}

JoinSide JoinSide::GetJoinSide(idx_t table_binding, const unordered_set<idx_t> &left_bindings,
                               const unordered_set<idx_t> &right_bindings) {
	if (left_bindings.find(table_binding) != left_bindings.end()) {
		// column references table on left side
		D_ASSERT(right_bindings.find(table_binding) == right_bindings.end());
		return JoinSide::LEFT;
	} else {
		// column references table on right side
		D_ASSERT(right_bindings.find(table_binding) != right_bindings.end());
		return JoinSide::RIGHT;
	}
}

JoinSide JoinSide::GetJoinSide(Expression &expression, const unordered_set<idx_t> &left_bindings,
                               const unordered_set<idx_t> &right_bindings) {
	if (expression.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expression.Cast<BoundColumnRefExpression>();
		if (colref.depth > 0) {
			throw Exception("Non-inner join on correlated columns not supported");
		}
		return GetJoinSide(colref.binding.table_index, left_bindings, right_bindings);
	}
	D_ASSERT(expression.type != ExpressionType::BOUND_REF);
	if (expression.type == ExpressionType::SUBQUERY) {
		D_ASSERT(expression.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &subquery = expression.Cast<BoundSubqueryExpression>();
		JoinSide side = JoinSide::NONE;
		if (subquery.child) {
			side = GetJoinSide(*subquery.child, left_bindings, right_bindings);
		}
		// correlated subquery, check the side of each of correlated columns in the subquery
		for (auto &corr : subquery.binder->correlated_columns) {
			if (corr.depth > 1) {
				// correlated column has depth > 1
				// it does not refer to any table in the current set of bindings
				return JoinSide::BOTH;
			}
			auto correlated_side = GetJoinSide(corr.binding.table_index, left_bindings, right_bindings);
			side = CombineJoinSide(side, correlated_side);
		}
		return side;
	}
	JoinSide join_side = JoinSide::NONE;
	ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
		auto child_side = GetJoinSide(child, left_bindings, right_bindings);
		join_side = CombineJoinSide(child_side, join_side);
	});
	return join_side;
}

JoinSide JoinSide::GetJoinSide(const unordered_set<idx_t> &bindings, const unordered_set<idx_t> &left_bindings,
                               const unordered_set<idx_t> &right_bindings) {
	JoinSide side = JoinSide::NONE;
	for (auto binding : bindings) {
		side = CombineJoinSide(side, GetJoinSide(binding, left_bindings, right_bindings));
	}
	return side;
}

} // namespace duckdb











namespace duckdb {

LogicalOperator::LogicalOperator(LogicalOperatorType type)
    : type(type), estimated_cardinality(0), has_estimated_cardinality(false) {
}

LogicalOperator::LogicalOperator(LogicalOperatorType type, vector<unique_ptr<Expression>> expressions)
    : type(type), expressions(std::move(expressions)), estimated_cardinality(0), has_estimated_cardinality(false) {
}

LogicalOperator::~LogicalOperator() {
}

vector<ColumnBinding> LogicalOperator::GetColumnBindings() {
	return {ColumnBinding(0, 0)};
}

string LogicalOperator::GetName() const {
	return LogicalOperatorToString(type);
}

string LogicalOperator::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += expressions[i]->GetName();
	}
	return result;
}

void LogicalOperator::ResolveOperatorTypes() {

	types.clear();
	// first resolve child types
	for (auto &child : children) {
		child->ResolveOperatorTypes();
	}
	// now resolve the types for this operator
	ResolveTypes();
	D_ASSERT(types.size() == GetColumnBindings().size());
}

vector<ColumnBinding> LogicalOperator::GenerateColumnBindings(idx_t table_idx, idx_t column_count) {
	vector<ColumnBinding> result;
	result.reserve(column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result.emplace_back(table_idx, i);
	}
	return result;
}

vector<LogicalType> LogicalOperator::MapTypes(const vector<LogicalType> &types, const vector<idx_t> &projection_map) {
	if (projection_map.empty()) {
		return types;
	} else {
		vector<LogicalType> result_types;
		result_types.reserve(projection_map.size());
		for (auto index : projection_map) {
			result_types.push_back(types[index]);
		}
		return result_types;
	}
}

vector<ColumnBinding> LogicalOperator::MapBindings(const vector<ColumnBinding> &bindings,
                                                   const vector<idx_t> &projection_map) {
	if (projection_map.empty()) {
		return bindings;
	} else {
		vector<ColumnBinding> result_bindings;
		result_bindings.reserve(projection_map.size());
		for (auto index : projection_map) {
			D_ASSERT(index < bindings.size());
			result_bindings.push_back(bindings[index]);
		}
		return result_bindings;
	}
}

string LogicalOperator::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

void LogicalOperator::Verify(ClientContext &context) {
#ifdef DEBUG
	// verify expressions
	for (idx_t expr_idx = 0; expr_idx < expressions.size(); expr_idx++) {
		auto str = expressions[expr_idx]->ToString();
		// verify that we can (correctly) copy this expression
		auto copy = expressions[expr_idx]->Copy();
		auto original_hash = expressions[expr_idx]->Hash();
		auto copy_hash = copy->Hash();
		// copy should be identical to original
		D_ASSERT(expressions[expr_idx]->ToString() == copy->ToString());
		D_ASSERT(original_hash == copy_hash);
		D_ASSERT(Expression::Equals(expressions[expr_idx], copy));

		for (idx_t other_idx = 0; other_idx < expr_idx; other_idx++) {
			// comparison with other expressions
			auto other_hash = expressions[other_idx]->Hash();
			bool expr_equal = Expression::Equals(expressions[expr_idx], expressions[other_idx]);
			if (original_hash != other_hash) {
				// if the hashes are not equal the expressions should not be equal either
				D_ASSERT(!expr_equal);
			}
		}
		D_ASSERT(!str.empty());

		// verify that serialization + deserialization round-trips correctly
		if (expressions[expr_idx]->HasParameter()) {
			continue;
		}
		MemoryStream stream;
		// We are serializing a query plan
		try {
			BinarySerializer::Serialize(*expressions[expr_idx], stream);
		} catch (NotImplementedException &ex) {
			// ignore for now (FIXME)
			continue;
		}
		// Rewind the stream
		stream.Rewind();

		bound_parameter_map_t parameters;
		auto deserialized_expression = BinaryDeserializer::Deserialize<Expression>(stream, context, parameters);

		// FIXME: expressions might not be equal yet because of statistics propagation
		continue;
		D_ASSERT(Expression::Equals(expressions[expr_idx], deserialized_expression));
		D_ASSERT(expressions[expr_idx]->Hash() == deserialized_expression->Hash());
	}
	D_ASSERT(!ToString().empty());
	for (auto &child : children) {
		child->Verify(context);
	}
#endif
}

void LogicalOperator::AddChild(unique_ptr<LogicalOperator> child) {
	D_ASSERT(child);
	children.push_back(std::move(child));
}

idx_t LogicalOperator::EstimateCardinality(ClientContext &context) {
	// simple estimator, just take the max of the children
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	idx_t max_cardinality = 0;
	for (auto &child : children) {
		max_cardinality = MaxValue(child->EstimateCardinality(context), max_cardinality);
	}
	has_estimated_cardinality = true;
	estimated_cardinality = max_cardinality;
	return estimated_cardinality;
}

void LogicalOperator::Print() {
	Printer::Print(ToString());
}

vector<idx_t> LogicalOperator::GetTableIndex() const {
	return vector<idx_t> {};
}

unique_ptr<LogicalOperator> LogicalOperator::Copy(ClientContext &context) const {
	MemoryStream stream;
	BinarySerializer serializer(stream);
	try {
		serializer.Begin();
		this->Serialize(serializer);
		serializer.End();
	} catch (NotImplementedException &ex) {
		throw NotImplementedException("Logical Operator Copy requires the logical operator and all of its children to "
		                              "be serializable: " +
		                              std::string(ex.what()));
	}
	stream.Rewind();
	bound_parameter_map_t parameters;
	auto op_copy = BinaryDeserializer::Deserialize<LogicalOperator>(stream, context, parameters);
	return op_copy;
}

} // namespace duckdb






namespace duckdb {

void LogicalOperatorVisitor::VisitOperator(LogicalOperator &op) {
	VisitOperatorChildren(op);
	VisitOperatorExpressions(op);
}

void LogicalOperatorVisitor::VisitOperatorChildren(LogicalOperator &op) {
	for (auto &child : op.children) {
		VisitOperator(*child);
	}
}

void LogicalOperatorVisitor::EnumerateExpressions(LogicalOperator &op,
                                                  const std::function<void(unique_ptr<Expression> *child)> &callback) {

	switch (op.type) {
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		auto &get = op.Cast<LogicalExpressionGet>();
		for (auto &expr_list : get.expressions) {
			for (auto &expr : expr_list) {
				callback(&expr);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = op.Cast<LogicalOrder>();
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &order = op.Cast<LogicalTopN>();
		for (auto &node : order.orders) {
			callback(&node.expression);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = op.Cast<LogicalDistinct>();
		for (auto &target : distinct.distinct_targets) {
			callback(&target);
		}
		if (distinct.order_by) {
			for (auto &order : distinct.order_by->orders) {
				callback(&order.expression);
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		auto &insert = op.Cast<LogicalInsert>();
		if (insert.on_conflict_condition) {
			callback(&insert.on_conflict_condition);
		}
		if (insert.do_update_condition) {
			callback(&insert.do_update_condition);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &expr : join.duplicate_eliminated_columns) {
			callback(&expr);
		}
		for (auto &cond : join.conditions) {
			callback(&cond.left);
			callback(&cond.right);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		auto &join = op.Cast<LogicalAnyJoin>();
		callback(&join.condition);
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = op.Cast<LogicalLimit>();
		if (limit.limit) {
			callback(&limit.limit);
		}
		if (limit.offset) {
			callback(&limit.offset);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT: {
		auto &limit = op.Cast<LogicalLimitPercent>();
		if (limit.limit) {
			callback(&limit.limit);
		}
		if (limit.offset) {
			callback(&limit.offset);
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = op.Cast<LogicalAggregate>();
		for (auto &group : aggr.groups) {
			callback(&group);
		}
		break;
	}
	default:
		break;
	}
	for (auto &expression : op.expressions) {
		callback(&expression);
	}
}

void LogicalOperatorVisitor::VisitOperatorExpressions(LogicalOperator &op) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *child) { VisitExpression(child); });
}

void LogicalOperatorVisitor::VisitExpression(unique_ptr<Expression> *expression) {
	auto &expr = **expression;
	unique_ptr<Expression> result;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_AGGREGATE:
		result = VisitReplace(expr.Cast<BoundAggregateExpression>(), expression);
		break;
	case ExpressionClass::BOUND_BETWEEN:
		result = VisitReplace(expr.Cast<BoundBetweenExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CASE:
		result = VisitReplace(expr.Cast<BoundCaseExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CAST:
		result = VisitReplace(expr.Cast<BoundCastExpression>(), expression);
		break;
	case ExpressionClass::BOUND_COLUMN_REF:
		result = VisitReplace(expr.Cast<BoundColumnRefExpression>(), expression);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		result = VisitReplace(expr.Cast<BoundComparisonExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		result = VisitReplace(expr.Cast<BoundConjunctionExpression>(), expression);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		result = VisitReplace(expr.Cast<BoundConstantExpression>(), expression);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		result = VisitReplace(expr.Cast<BoundFunctionExpression>(), expression);
		break;
	case ExpressionClass::BOUND_SUBQUERY:
		result = VisitReplace(expr.Cast<BoundSubqueryExpression>(), expression);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		result = VisitReplace(expr.Cast<BoundOperatorExpression>(), expression);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		result = VisitReplace(expr.Cast<BoundParameterExpression>(), expression);
		break;
	case ExpressionClass::BOUND_REF:
		result = VisitReplace(expr.Cast<BoundReferenceExpression>(), expression);
		break;
	case ExpressionClass::BOUND_DEFAULT:
		result = VisitReplace(expr.Cast<BoundDefaultExpression>(), expression);
		break;
	case ExpressionClass::BOUND_WINDOW:
		result = VisitReplace(expr.Cast<BoundWindowExpression>(), expression);
		break;
	case ExpressionClass::BOUND_UNNEST:
		result = VisitReplace(expr.Cast<BoundUnnestExpression>(), expression);
		break;
	default:
		throw InternalException("Unrecognized expression type in logical operator visitor");
	}
	if (result) {
		*expression = std::move(result);
	} else {
		// visit the children of this node
		VisitExpressionChildren(expr);
	}
}

void LogicalOperatorVisitor::VisitExpressionChildren(Expression &expr) {
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &expr) { VisitExpression(&expr); });
}

// these are all default methods that can be overriden
// we don't care about coverage here
// LCOV_EXCL_START
unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundAggregateExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundBetweenExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundCaseExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundCastExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundComparisonExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundConjunctionExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundConstantExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundDefaultExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundFunctionExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundOperatorExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundParameterExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundReferenceExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundSubqueryExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundWindowExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

unique_ptr<Expression> LogicalOperatorVisitor::VisitReplace(BoundUnnestExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	return nullptr;
}

// LCOV_EXCL_STOP

} // namespace duckdb





namespace duckdb {

LogicalAggregate::LogicalAggregate(idx_t group_index, idx_t aggregate_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY, std::move(select_list)),
      group_index(group_index), aggregate_index(aggregate_index), groupings_index(DConstants::INVALID_INDEX) {
}

void LogicalAggregate::ResolveTypes() {
	D_ASSERT(groupings_index != DConstants::INVALID_INDEX || grouping_functions.empty());
	for (auto &expr : groups) {
		types.push_back(expr->return_type);
	}
	// get the chunk types from the projection list
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
	for (idx_t i = 0; i < grouping_functions.size(); i++) {
		types.emplace_back(LogicalType::BIGINT);
	}
}

vector<ColumnBinding> LogicalAggregate::GetColumnBindings() {
	D_ASSERT(groupings_index != DConstants::INVALID_INDEX || grouping_functions.empty());
	vector<ColumnBinding> result;
	result.reserve(groups.size() + expressions.size() + grouping_functions.size());
	for (idx_t i = 0; i < groups.size(); i++) {
		result.emplace_back(group_index, i);
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		result.emplace_back(aggregate_index, i);
	}
	for (idx_t i = 0; i < grouping_functions.size(); i++) {
		result.emplace_back(groupings_index, i);
	}
	return result;
}

string LogicalAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += expressions[i]->GetName();
	}
	return result;
}

idx_t LogicalAggregate::EstimateCardinality(ClientContext &context) {
	if (groups.empty()) {
		// ungrouped aggregate
		return 1;
	}
	return LogicalOperator::EstimateCardinality(context);
}

vector<idx_t> LogicalAggregate::GetTableIndex() const {
	vector<idx_t> result {group_index, aggregate_index};
	if (groupings_index != DConstants::INVALID_INDEX) {
		result.push_back(groupings_index);
	}
	return result;
}

string LogicalAggregate::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() +
		       StringUtil::Format(" #%llu, #%llu, #%llu", group_index, aggregate_index, groupings_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb


namespace duckdb {

LogicalAnyJoin::LogicalAnyJoin(JoinType type) : LogicalJoin(type, LogicalOperatorType::LOGICAL_ANY_JOIN) {
}

string LogicalAnyJoin::ParamsToString() const {
	return condition->ToString();
}

} // namespace duckdb





namespace duckdb {

LogicalColumnDataGet::LogicalColumnDataGet(idx_t table_index, vector<LogicalType> types,
                                           unique_ptr<ColumnDataCollection> collection)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CHUNK_GET), table_index(table_index),
      collection(std::move(collection)) {
	D_ASSERT(types.size() > 0);
	chunk_types = std::move(types);
}

vector<ColumnBinding> LogicalColumnDataGet::GetColumnBindings() {
	return GenerateColumnBindings(table_index, chunk_types.size());
}

vector<idx_t> LogicalColumnDataGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalColumnDataGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb




namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

string LogicalComparisonJoin::ParamsToString() const {
	string result = EnumUtil::ToChars(join_type);
	for (auto &condition : conditions) {
		result += "\n";
		auto expr =
		    make_uniq<BoundComparisonExpression>(condition.comparison, condition.left->Copy(), condition.right->Copy());
		result += expr->ToString();
	}

	return result;
}

} // namespace duckdb





namespace duckdb {

void LogicalCopyToFile::Serialize(Serializer &serializer) const {
	throw SerializationException("LogicalCopyToFile not implemented yet");
}

unique_ptr<LogicalOperator> LogicalCopyToFile::Deserialize(Deserializer &deserializer) {
	throw SerializationException("LogicalCopyToFile not implemented yet");
}

idx_t LogicalCopyToFile::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb


namespace duckdb {

LogicalCreate::LogicalCreate(LogicalOperatorType type, unique_ptr<CreateInfo> info,
                             optional_ptr<SchemaCatalogEntry> schema)
    : LogicalOperator(type), schema(schema), info(std::move(info)) {
}

LogicalCreate::LogicalCreate(LogicalOperatorType type, ClientContext &context, unique_ptr<CreateInfo> info_p)
    : LogicalOperator(type), info(std::move(info_p)) {
	this->schema = Catalog::GetSchema(context, info->catalog, info->schema, OnEntryNotFound::RETURN_NULL);
}

idx_t LogicalCreate::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalCreate::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb






namespace duckdb {

LogicalCreateIndex::LogicalCreateIndex(unique_ptr<CreateIndexInfo> info_p, vector<unique_ptr<Expression>> expressions_p,
                                       TableCatalogEntry &table_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX), info(std::move(info_p)), table(table_p) {

	for (auto &expr : expressions_p) {
		this->unbound_expressions.push_back(expr->Copy());
	}
	this->expressions = std::move(expressions_p);

	if (info->column_ids.empty()) {
		throw BinderException("CREATE INDEX does not refer to any columns in the base table!");
	}
}

LogicalCreateIndex::LogicalCreateIndex(ClientContext &context, unique_ptr<CreateInfo> info_p,
                                       vector<unique_ptr<Expression>> expressions_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_INDEX),
      info(unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(info_p))), table(BindTable(context, *info)) {
	for (auto &expr : expressions_p) {
		this->unbound_expressions.push_back(expr->Copy());
	}
	this->expressions = std::move(expressions_p);
}

void LogicalCreateIndex::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

TableCatalogEntry &LogicalCreateIndex::BindTable(ClientContext &context, CreateIndexInfo &info) {
	auto &catalog = info.catalog;
	auto &schema = info.schema;
	auto &table_name = info.table;
	return Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table_name);
}

} // namespace duckdb


namespace duckdb {

LogicalCreateTable::LogicalCreateTable(SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_TABLE), schema(schema), info(std::move(info)) {
}

LogicalCreateTable::LogicalCreateTable(ClientContext &context, unique_ptr<CreateInfo> unbound_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_CREATE_TABLE),
      schema(Catalog::GetSchema(context, unbound_info->catalog, unbound_info->schema)) {
	D_ASSERT(unbound_info->type == CatalogType::TABLE_ENTRY);
	auto binder = Binder::CreateBinder(context);
	info = binder->BindCreateTableInfo(unique_ptr_cast<CreateInfo, CreateTableInfo>(std::move(unbound_info)));
}

idx_t LogicalCreateTable::EstimateCardinality(ClientContext &context) {
	return 1;
}

void LogicalCreateTable::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

} // namespace duckdb


namespace duckdb {

LogicalCrossProduct::LogicalCrossProduct(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_CROSS_PRODUCT, std::move(left), std::move(right)) {
}

unique_ptr<LogicalOperator> LogicalCrossProduct::Create(unique_ptr<LogicalOperator> left,
                                                        unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_uniq<LogicalCrossProduct>(std::move(left), std::move(right));
}

} // namespace duckdb




namespace duckdb {

vector<idx_t> LogicalCTERef::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalCTERef::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb






namespace duckdb {

LogicalDelete::LogicalDelete(TableCatalogEntry &table, idx_t table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE), table(table), table_index(table_index),
      return_chunk(false) {
}

LogicalDelete::LogicalDelete(ClientContext &context, const unique_ptr<CreateInfo> &table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 dynamic_cast<CreateTableInfo &>(*table_info).table)) {
}

idx_t LogicalDelete::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<idx_t> LogicalDelete::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

vector<ColumnBinding> LogicalDelete::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalDelete::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalDelete::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb




namespace duckdb {

vector<idx_t> LogicalDelimGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalDelimGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb


namespace duckdb {

LogicalDependentJoin::LogicalDependentJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right,
                                           vector<CorrelatedColumnInfo> correlated_columns, JoinType type,
                                           unique_ptr<Expression> condition)
    : LogicalComparisonJoin(type, LogicalOperatorType::LOGICAL_DEPENDENT_JOIN), join_condition(std::move(condition)),
      correlated_columns(std::move(correlated_columns)) {
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

unique_ptr<LogicalOperator> LogicalDependentJoin::Create(unique_ptr<LogicalOperator> left,
                                                         unique_ptr<LogicalOperator> right,
                                                         vector<CorrelatedColumnInfo> correlated_columns, JoinType type,
                                                         unique_ptr<Expression> condition) {
	return make_uniq<LogicalDependentJoin>(std::move(left), std::move(right), std::move(correlated_columns), type,
	                                       std::move(condition));
}

} // namespace duckdb



namespace duckdb {

LogicalDistinct::LogicalDistinct(DistinctType distinct_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_type(distinct_type) {
}
LogicalDistinct::LogicalDistinct(vector<unique_ptr<Expression>> targets, DistinctType distinct_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_DISTINCT), distinct_type(distinct_type),
      distinct_targets(std::move(targets)) {
}

string LogicalDistinct::ParamsToString() const {
	string result = LogicalOperator::ParamsToString();
	if (!distinct_targets.empty()) {
		result += StringUtil::Join(distinct_targets, distinct_targets.size(), "\n",
		                           [](const unique_ptr<Expression> &child) { return child->GetName(); });
	}

	return result;
}

void LogicalDistinct::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb




namespace duckdb {

vector<idx_t> LogicalDummyScan::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalDummyScan::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb


namespace duckdb {

LogicalEmptyResult::LogicalEmptyResult(unique_ptr<LogicalOperator> op)
    : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {

	this->bindings = op->GetColumnBindings();

	op->ResolveOperatorTypes();
	this->return_types = op->types;
}

LogicalEmptyResult::LogicalEmptyResult() : LogicalOperator(LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
}

} // namespace duckdb




namespace duckdb {

vector<idx_t> LogicalExpressionGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalExpressionGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb






namespace duckdb {

void LogicalExtensionOperator::ResolveColumnBindings(ColumnBindingResolver &res, vector<ColumnBinding> &bindings) {
	// general case
	// first visit the children of this operator
	for (auto &child : children) {
		res.VisitOperator(*child);
	}
	// now visit the expressions of this operator to resolve any bound column references
	for (auto &expression : expressions) {
		res.VisitExpression(&expression);
	}
	// finally update the current set of bindings to the current set of column bindings
	bindings = GetColumnBindings();
}

void LogicalExtensionOperator::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty(200, "extension_name", GetExtensionName());
}

unique_ptr<LogicalOperator> LogicalExtensionOperator::Deserialize(Deserializer &deserializer) {
	auto &config = DBConfig::GetConfig(deserializer.Get<ClientContext &>());
	auto extension_name = deserializer.ReadProperty<string>(200, "extension_name");
	for (auto &extension : config.operator_extensions) {
		if (extension->GetName() == extension_name) {
			return extension->Deserialize(deserializer);
		}
	}
	throw SerializationException("No deserialization method exists for extension: " + extension_name);
}

string LogicalExtensionOperator::GetExtensionName() const {
	throw SerializationException("LogicalExtensionOperator::GetExtensionName not implemented which is required for "
	                             "serializing extension operators");
}

} // namespace duckdb



namespace duckdb {

LogicalFilter::LogicalFilter(unique_ptr<Expression> expression) : LogicalOperator(LogicalOperatorType::LOGICAL_FILTER) {
	expressions.push_back(std::move(expression));
	SplitPredicates(expressions);
}

LogicalFilter::LogicalFilter() : LogicalOperator(LogicalOperatorType::LOGICAL_FILTER) {
}

void LogicalFilter::ResolveTypes() {
	types = MapTypes(children[0]->types, projection_map);
}

vector<ColumnBinding> LogicalFilter::GetColumnBindings() {
	return MapBindings(children[0]->GetColumnBindings(), projection_map);
}

// Split the predicates separated by AND statements
// These are the predicates that are safe to push down because all of them MUST
// be true
bool LogicalFilter::SplitPredicates(vector<unique_ptr<Expression>> &expressions) {
	bool found_conjunction = false;
	for (idx_t i = 0; i < expressions.size(); i++) {
		if (expressions[i]->type == ExpressionType::CONJUNCTION_AND) {
			auto &conjunction = expressions[i]->Cast<BoundConjunctionExpression>();
			found_conjunction = true;
			// AND expression, append the other children
			for (idx_t k = 1; k < conjunction.children.size(); k++) {
				expressions.push_back(std::move(conjunction.children[k]));
			}
			// replace this expression with the first child of the conjunction
			expressions[i] = std::move(conjunction.children[0]);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
		}
	}
	return found_conjunction;
}

} // namespace duckdb












namespace duckdb {

LogicalGet::LogicalGet() : LogicalOperator(LogicalOperatorType::LOGICAL_GET) {
}

LogicalGet::LogicalGet(idx_t table_index, TableFunction function, unique_ptr<FunctionData> bind_data,
                       vector<LogicalType> returned_types, vector<string> returned_names)
    : LogicalOperator(LogicalOperatorType::LOGICAL_GET), table_index(table_index), function(std::move(function)),
      bind_data(std::move(bind_data)), returned_types(std::move(returned_types)), names(std::move(returned_names)),
      extra_info() {
}

optional_ptr<TableCatalogEntry> LogicalGet::GetTable() const {
	return TableScanFunction::GetTableEntry(function, bind_data.get());
}

string LogicalGet::ParamsToString() const {
	string result = "";
	for (auto &kv : table_filters.filters) {
		auto &column_index = kv.first;
		auto &filter = kv.second;
		if (column_index < names.size()) {
			result += filter->ToString(names[column_index]);
		}
		result += "\n";
	}
	if (!extra_info.file_filters.empty()) {
		result += "\n[INFOSEPARATOR]\n";
		result += "File Filters: " + extra_info.file_filters;
	}
	if (!function.to_string) {
		return result;
	}
	return result + "\n" + function.to_string(bind_data.get());
}

vector<ColumnBinding> LogicalGet::GetColumnBindings() {
	if (column_ids.empty()) {
		return {ColumnBinding(table_index, 0)};
	}
	vector<ColumnBinding> result;
	if (projection_ids.empty()) {
		for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
			result.emplace_back(table_index, col_idx);
		}
	} else {
		for (auto proj_id : projection_ids) {
			result.emplace_back(table_index, proj_id);
		}
	}
	if (!projected_input.empty()) {
		if (children.size() != 1) {
			throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
		}
		auto child_bindings = children[0]->GetColumnBindings();
		for (auto entry : projected_input) {
			D_ASSERT(entry < child_bindings.size());
			result.emplace_back(child_bindings[entry]);
		}
	}
	return result;
}

void LogicalGet::ResolveTypes() {
	if (column_ids.empty()) {
		column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	}

	if (projection_ids.empty()) {
		for (auto &index : column_ids) {
			if (index == COLUMN_IDENTIFIER_ROW_ID) {
				types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				types.push_back(returned_types[index]);
			}
		}
	} else {
		for (auto &proj_index : projection_ids) {
			auto &index = column_ids[proj_index];
			if (index == COLUMN_IDENTIFIER_ROW_ID) {
				types.emplace_back(LogicalType::ROW_TYPE);
			} else {
				types.push_back(returned_types[index]);
			}
		}
	}
	if (!projected_input.empty()) {
		if (children.size() != 1) {
			throw InternalException("LogicalGet::project_input can only be set for table-in-out functions");
		}
		for (auto entry : projected_input) {
			D_ASSERT(entry < children[0]->types.size());
			types.push_back(children[0]->types[entry]);
		}
	}
}

idx_t LogicalGet::EstimateCardinality(ClientContext &context) {
	// join order optimizer does better cardinality estimation.
	if (has_estimated_cardinality) {
		return estimated_cardinality;
	}
	if (function.cardinality) {
		auto node_stats = function.cardinality(context, bind_data.get());
		if (node_stats && node_stats->has_estimated_cardinality) {
			return node_stats->estimated_cardinality;
		}
	}
	return 1;
}

void LogicalGet::Serialize(Serializer &serializer) const {
	LogicalOperator::Serialize(serializer);
	serializer.WriteProperty(200, "table_index", table_index);
	serializer.WriteProperty(201, "returned_types", returned_types);
	serializer.WriteProperty(202, "names", names);
	serializer.WriteProperty(203, "column_ids", column_ids);
	serializer.WriteProperty(204, "projection_ids", projection_ids);
	serializer.WriteProperty(205, "table_filters", table_filters);
	FunctionSerializer::Serialize(serializer, function, bind_data.get());
	if (!function.serialize) {
		D_ASSERT(!function.serialize);
		// no serialize method: serialize input values and named_parameters for rebinding purposes
		serializer.WriteProperty(206, "parameters", parameters);
		serializer.WriteProperty(207, "named_parameters", named_parameters);
		serializer.WriteProperty(208, "input_table_types", input_table_types);
		serializer.WriteProperty(209, "input_table_names", input_table_names);
	}
	serializer.WriteProperty(210, "projected_input", projected_input);
}

unique_ptr<LogicalOperator> LogicalGet::Deserialize(Deserializer &deserializer) {
	auto result = unique_ptr<LogicalGet>(new LogicalGet());
	deserializer.ReadProperty(200, "table_index", result->table_index);
	deserializer.ReadProperty(201, "returned_types", result->returned_types);
	deserializer.ReadProperty(202, "names", result->names);
	deserializer.ReadProperty(203, "column_ids", result->column_ids);
	deserializer.ReadProperty(204, "projection_ids", result->projection_ids);
	deserializer.ReadProperty(205, "table_filters", result->table_filters);
	auto entry = FunctionSerializer::DeserializeBase<TableFunction, TableFunctionCatalogEntry>(
	    deserializer, CatalogType::TABLE_FUNCTION_ENTRY);
	result->function = entry.first;
	auto &function = result->function;
	auto has_serialize = entry.second;

	unique_ptr<FunctionData> bind_data;
	if (!has_serialize) {
		deserializer.ReadProperty(206, "parameters", result->parameters);
		deserializer.ReadProperty(207, "named_parameters", result->named_parameters);
		deserializer.ReadProperty(208, "input_table_types", result->input_table_types);
		deserializer.ReadProperty(209, "input_table_names", result->input_table_names);
		TableFunctionBindInput input(result->parameters, result->named_parameters, result->input_table_types,
		                             result->input_table_names, function.function_info.get());

		vector<LogicalType> bind_return_types;
		vector<string> bind_names;
		if (!function.bind) {
			throw InternalException("Table function \"%s\" has neither bind nor (de)serialize", function.name);
		}
		bind_data = function.bind(deserializer.Get<ClientContext &>(), input, bind_return_types, bind_names);
		if (result->returned_types != bind_return_types) {
			throw SerializationException(
			    "Table function deserialization failure - bind returned different return types than were serialized");
		}
		// names can actually be different because of aliases - only the sizes cannot be different
		if (result->names.size() != bind_names.size()) {
			throw SerializationException(
			    "Table function deserialization failure - bind returned different returned names than were serialized");
		}
	} else {
		bind_data = FunctionSerializer::FunctionDeserialize(deserializer, function);
	}
	result->bind_data = std::move(bind_data);
	deserializer.ReadProperty(210, "projected_input", result->projected_input);
	return std::move(result);
}

vector<idx_t> LogicalGet::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalGet::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return StringUtil::Upper(function.name) + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return StringUtil::Upper(function.name);
}

} // namespace duckdb






namespace duckdb {

LogicalInsert::LogicalInsert(TableCatalogEntry &table, idx_t table_index)
    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT), table(table), table_index(table_index), return_chunk(false),
      action_type(OnConflictAction::THROW) {
}

LogicalInsert::LogicalInsert(ClientContext &context, const unique_ptr<CreateInfo> table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_INSERT),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 dynamic_cast<CreateTableInfo &>(*table_info).table)) {
}

idx_t LogicalInsert::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<idx_t> LogicalInsert::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

vector<ColumnBinding> LogicalInsert::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalInsert::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalInsert::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb





namespace duckdb {

LogicalJoin::LogicalJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalOperator(logical_type), join_type(join_type) {
}

vector<ColumnBinding> LogicalJoin::GetColumnBindings() {
	auto left_bindings = MapBindings(children[0]->GetColumnBindings(), left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return left_bindings;
	}
	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side plus the MARK column
		left_bindings.emplace_back(mark_index, 0);
		return left_bindings;
	}
	// for other join types we project both the LHS and the RHS
	auto right_bindings = MapBindings(children[1]->GetColumnBindings(), right_projection_map);
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalJoin::ResolveTypes() {
	types = MapTypes(children[0]->types, left_projection_map);
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		// for SEMI and ANTI join we only project the left hand side
		return;
	}
	if (join_type == JoinType::MARK) {
		// for MARK join we project the left hand side, plus a BOOLEAN column indicating the MARK
		types.emplace_back(LogicalType::BOOLEAN);
		return;
	}
	// for any other join we project both sides
	auto right_types = MapTypes(children[1]->types, right_projection_map);
	types.insert(types.end(), right_types.begin(), right_types.end());
}

void LogicalJoin::GetTableReferences(LogicalOperator &op, unordered_set<idx_t> &bindings) {
	auto column_bindings = op.GetColumnBindings();
	for (auto binding : column_bindings) {
		bindings.insert(binding.table_index);
	}
}

void LogicalJoin::GetExpressionBindings(Expression &expr, unordered_set<idx_t> &bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.depth == 0);
		bindings.insert(colref.binding.table_index);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { GetExpressionBindings(child, bindings); });
}

} // namespace duckdb


namespace duckdb {

LogicalLimit::LogicalLimit(int64_t limit_val, int64_t offset_val, unique_ptr<Expression> limit,
                           unique_ptr<Expression> offset)
    : LogicalOperator(LogicalOperatorType::LOGICAL_LIMIT), limit_val(limit_val), offset_val(offset_val),
      limit(std::move(limit)), offset(std::move(offset)) {
}

vector<ColumnBinding> LogicalLimit::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

idx_t LogicalLimit::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = children[0]->EstimateCardinality(context);
	if (limit_val >= 0 && idx_t(limit_val) < child_cardinality) {
		child_cardinality = limit_val;
	}
	return child_cardinality;
}

void LogicalLimit::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb

#include <cmath>

namespace duckdb {

idx_t LogicalLimitPercent::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if ((limit_percent < 0 || limit_percent > 100) || std::isnan(limit_percent)) {
		return child_cardinality;
	}
	return idx_t(child_cardinality * (limit_percent / 100.0));
}

} // namespace duckdb


namespace duckdb {

vector<idx_t> LogicalMaterializedCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

} // namespace duckdb


namespace duckdb {

LogicalOrder::LogicalOrder(vector<BoundOrderByNode> orders)
    : LogicalOperator(LogicalOperatorType::LOGICAL_ORDER_BY), orders(std::move(orders)) {
}

vector<ColumnBinding> LogicalOrder::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	if (projections.empty()) {
		return child_bindings;
	}

	vector<ColumnBinding> result;
	for (auto &col_idx : projections) {
		result.push_back(child_bindings[col_idx]);
	}
	return result;
}

string LogicalOrder::ParamsToString() const {
	string result = "ORDERS:\n";
	for (idx_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += orders[i].expression->GetName();
	}
	return result;
}

void LogicalOrder::ResolveTypes() {
	const auto child_types = children[0]->types;
	if (projections.empty()) {
		types = child_types;
	} else {
		for (auto &col_idx : projections) {
			types.push_back(child_types[col_idx]);
		}
	}
}

} // namespace duckdb




namespace duckdb {

LogicalPivot::LogicalPivot() : LogicalOperator(LogicalOperatorType::LOGICAL_PIVOT) {
}

LogicalPivot::LogicalPivot(idx_t pivot_idx, unique_ptr<LogicalOperator> plan, BoundPivotInfo info_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PIVOT), pivot_index(pivot_idx), bound_pivot(std::move(info_p)) {
	D_ASSERT(plan);
	children.push_back(std::move(plan));
}

vector<ColumnBinding> LogicalPivot::GetColumnBindings() {
	vector<ColumnBinding> result;
	for (idx_t i = 0; i < bound_pivot.types.size(); i++) {
		result.emplace_back(pivot_index, i);
	}
	return result;
}

vector<idx_t> LogicalPivot::GetTableIndex() const {
	return vector<idx_t> {pivot_index};
}

void LogicalPivot::ResolveTypes() {
	this->types = bound_pivot.types;
}

string LogicalPivot::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", pivot_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb


namespace duckdb {

LogicalPositionalJoin::LogicalPositionalJoin(unique_ptr<LogicalOperator> left, unique_ptr<LogicalOperator> right)
    : LogicalUnconditionalJoin(LogicalOperatorType::LOGICAL_POSITIONAL_JOIN, std::move(left), std::move(right)) {
}

unique_ptr<LogicalOperator> LogicalPositionalJoin::Create(unique_ptr<LogicalOperator> left,
                                                          unique_ptr<LogicalOperator> right) {
	if (left->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return right;
	}
	if (right->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN) {
		return left;
	}
	return make_uniq<LogicalPositionalJoin>(std::move(left), std::move(right));
}

} // namespace duckdb


namespace duckdb {

idx_t LogicalPragma::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb


namespace duckdb {

idx_t LogicalPrepare::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb




namespace duckdb {

LogicalProjection::LogicalProjection(idx_t table_index, vector<unique_ptr<Expression>> select_list)
    : LogicalOperator(LogicalOperatorType::LOGICAL_PROJECTION, std::move(select_list)), table_index(table_index) {
}

vector<ColumnBinding> LogicalProjection::GetColumnBindings() {
	return GenerateColumnBindings(table_index, expressions.size());
}

void LogicalProjection::ResolveTypes() {
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalProjection::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalProjection::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb




namespace duckdb {

vector<idx_t> LogicalRecursiveCTE::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalRecursiveCTE::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb


namespace duckdb {

idx_t LogicalReset::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb


namespace duckdb {

LogicalSample::LogicalSample() : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE) {
}

LogicalSample::LogicalSample(unique_ptr<SampleOptions> sample_options_p, unique_ptr<LogicalOperator> child)
    : LogicalOperator(LogicalOperatorType::LOGICAL_SAMPLE), sample_options(std::move(sample_options_p)) {
	children.push_back(std::move(child));
}

vector<ColumnBinding> LogicalSample::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

idx_t LogicalSample::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = children[0]->EstimateCardinality(context);
	if (sample_options->is_percentage) {
		double sample_cardinality =
		    double(child_cardinality) * (sample_options->sample_size.GetValue<double>() / 100.0);
		if (sample_cardinality > double(child_cardinality)) {
			return child_cardinality;
		}
		return idx_t(sample_cardinality);
	} else {
		auto sample_size = sample_options->sample_size.GetValue<uint64_t>();
		if (sample_size < child_cardinality) {
			return sample_size;
		}
	}
	return child_cardinality;
}

void LogicalSample::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb


namespace duckdb {

idx_t LogicalSet::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb




namespace duckdb {

vector<idx_t> LogicalSetOperation::GetTableIndex() const {
	return vector<idx_t> {table_index};
}

string LogicalSetOperation::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb









namespace duckdb {

idx_t LogicalSimple::EstimateCardinality(ClientContext &context) {
	return 1;
}

} // namespace duckdb


namespace duckdb {

idx_t LogicalTopN::EstimateCardinality(ClientContext &context) {
	auto child_cardinality = LogicalOperator::EstimateCardinality(context);
	if (limit >= 0 && child_cardinality < idx_t(limit)) {
		return limit;
	}
	return child_cardinality;
}

} // namespace duckdb


namespace duckdb {

LogicalUnconditionalJoin::LogicalUnconditionalJoin(LogicalOperatorType logical_type, unique_ptr<LogicalOperator> left,
                                                   unique_ptr<LogicalOperator> right)
    : LogicalOperator(logical_type) {
	D_ASSERT(left);
	D_ASSERT(right);
	children.push_back(std::move(left));
	children.push_back(std::move(right));
}

vector<ColumnBinding> LogicalUnconditionalJoin::GetColumnBindings() {
	auto left_bindings = children[0]->GetColumnBindings();
	auto right_bindings = children[1]->GetColumnBindings();
	left_bindings.insert(left_bindings.end(), right_bindings.begin(), right_bindings.end());
	return left_bindings;
}

void LogicalUnconditionalJoin::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	types.insert(types.end(), children[1]->types.begin(), children[1]->types.end());
}

} // namespace duckdb




namespace duckdb {

vector<ColumnBinding> LogicalUnnest::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	for (idx_t i = 0; i < expressions.size(); i++) {
		child_bindings.emplace_back(unnest_index, i);
	}
	return child_bindings;
}

void LogicalUnnest::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalUnnest::GetTableIndex() const {
	return vector<idx_t> {unnest_index};
}

string LogicalUnnest::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", unnest_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb





namespace duckdb {

LogicalUpdate::LogicalUpdate(TableCatalogEntry &table)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE), table(table), table_index(0), return_chunk(false) {
}

LogicalUpdate::LogicalUpdate(ClientContext &context, const unique_ptr<CreateInfo> &table_info)
    : LogicalOperator(LogicalOperatorType::LOGICAL_UPDATE),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 dynamic_cast<CreateTableInfo &>(*table_info).table)) {
}

idx_t LogicalUpdate::EstimateCardinality(ClientContext &context) {
	return return_chunk ? LogicalOperator::EstimateCardinality(context) : 1;
}

vector<ColumnBinding> LogicalUpdate::GetColumnBindings() {
	if (return_chunk) {
		return GenerateColumnBindings(table_index, table.GetTypes().size());
	}
	return {ColumnBinding(0, 0)};
}

void LogicalUpdate::ResolveTypes() {
	if (return_chunk) {
		types = table.GetTypes();
	} else {
		types.emplace_back(LogicalType::BIGINT);
	}
}

string LogicalUpdate::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", table_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb




namespace duckdb {

vector<ColumnBinding> LogicalWindow::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	for (idx_t i = 0; i < expressions.size(); i++) {
		child_bindings.emplace_back(window_index, i);
	}
	return child_bindings;
}

void LogicalWindow::ResolveTypes() {
	types.insert(types.end(), children[0]->types.begin(), children[0]->types.end());
	for (auto &expr : expressions) {
		types.push_back(expr->return_type);
	}
}

vector<idx_t> LogicalWindow::GetTableIndex() const {
	return vector<idx_t> {window_index};
}

string LogicalWindow::GetName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return LogicalOperator::GetName() + StringUtil::Format(" #%llu", window_index);
	}
#endif
	return LogicalOperator::GetName();
}

} // namespace duckdb















namespace duckdb {

Planner::Planner(ClientContext &context) : binder(Binder::CreateBinder(context)), context(context) {
}

static void CheckTreeDepth(const LogicalOperator &op, idx_t max_depth, idx_t depth = 0) {
	if (depth >= max_depth) {
		throw ParserException("Maximum tree depth of %lld exceeded in logical planner", max_depth);
	}
	for (auto &child : op.children) {
		CheckTreeDepth(*child, max_depth, depth + 1);
	}
}

void Planner::CreatePlan(SQLStatement &statement) {
	auto &profiler = QueryProfiler::Get(context);
	auto parameter_count = statement.n_param;

	BoundParameterMap bound_parameters(parameter_data);

	// first bind the tables and columns to the catalog
	bool parameters_resolved = true;
	try {
		profiler.StartPhase("binder");
		binder->parameters = &bound_parameters;
		auto bound_statement = binder->Bind(statement);
		profiler.EndPhase();

		this->names = bound_statement.names;
		this->types = bound_statement.types;
		this->plan = std::move(bound_statement.plan);

		auto max_tree_depth = ClientConfig::GetConfig(context).max_expression_depth;
		CheckTreeDepth(*plan, max_tree_depth);
	} catch (const ParameterNotResolvedException &ex) {
		// parameter types could not be resolved
		this->names = {"unknown"};
		this->types = {LogicalTypeId::UNKNOWN};
		this->plan = nullptr;
		parameters_resolved = false;
	} catch (const Exception &ex) {
		auto &config = DBConfig::GetConfig(context);

		this->plan = nullptr;
		for (auto &extension_op : config.operator_extensions) {
			auto bound_statement =
			    extension_op->Bind(context, *this->binder, extension_op->operator_info.get(), statement);
			if (bound_statement.plan != nullptr) {
				this->names = bound_statement.names;
				this->types = bound_statement.types;
				this->plan = std::move(bound_statement.plan);
				break;
			}
		}

		if (!this->plan) {
			throw;
		}
	} catch (std::exception &ex) {
		throw;
	}
	this->properties = binder->properties;
	this->properties.parameter_count = parameter_count;
	properties.bound_all_parameters = parameters_resolved;

	Planner::VerifyPlan(context, plan, bound_parameters.GetParametersPtr());

	// set up a map of parameter number -> value entries
	for (auto &kv : bound_parameters.GetParameters()) {
		auto &identifier = kv.first;
		auto &param = kv.second;
		// check if the type of the parameter could be resolved
		if (!param->return_type.IsValid()) {
			properties.bound_all_parameters = false;
			continue;
		}
		param->SetValue(Value(param->return_type));
		value_map[identifier] = param;
	}
}

shared_ptr<PreparedStatementData> Planner::PrepareSQLStatement(unique_ptr<SQLStatement> statement) {
	auto copied_statement = statement->Copy();
	// create a plan of the underlying statement
	CreatePlan(std::move(statement));
	// now create the logical prepare
	auto prepared_data = make_shared<PreparedStatementData>(copied_statement->type);
	prepared_data->unbound_statement = std::move(copied_statement);
	prepared_data->names = names;
	prepared_data->types = types;
	prepared_data->value_map = std::move(value_map);
	prepared_data->properties = properties;
	prepared_data->catalog_version = MetaTransaction::Get(context).catalog_version;
	return prepared_data;
}

void Planner::CreatePlan(unique_ptr<SQLStatement> statement) {
	D_ASSERT(statement);
	switch (statement->type) {
	case StatementType::SELECT_STATEMENT:
	case StatementType::INSERT_STATEMENT:
	case StatementType::COPY_STATEMENT:
	case StatementType::DELETE_STATEMENT:
	case StatementType::UPDATE_STATEMENT:
	case StatementType::CREATE_STATEMENT:
	case StatementType::DROP_STATEMENT:
	case StatementType::ALTER_STATEMENT:
	case StatementType::TRANSACTION_STATEMENT:
	case StatementType::EXPLAIN_STATEMENT:
	case StatementType::VACUUM_STATEMENT:
	case StatementType::RELATION_STATEMENT:
	case StatementType::CALL_STATEMENT:
	case StatementType::EXPORT_STATEMENT:
	case StatementType::PRAGMA_STATEMENT:
	case StatementType::SHOW_STATEMENT:
	case StatementType::SET_STATEMENT:
	case StatementType::LOAD_STATEMENT:
	case StatementType::EXTENSION_STATEMENT:
	case StatementType::PREPARE_STATEMENT:
	case StatementType::EXECUTE_STATEMENT:
	case StatementType::LOGICAL_PLAN_STATEMENT:
	case StatementType::ATTACH_STATEMENT:
	case StatementType::DETACH_STATEMENT:
		CreatePlan(*statement);
		break;
	default:
		throw NotImplementedException("Cannot plan statement of type %s!", StatementTypeToString(statement->type));
	}
}

static bool OperatorSupportsSerialization(LogicalOperator &op) {
	for (auto &child : op.children) {
		if (!OperatorSupportsSerialization(*child)) {
			return false;
		}
	}
	return op.SupportSerialization();
}

void Planner::VerifyPlan(ClientContext &context, unique_ptr<LogicalOperator> &op,
                         optional_ptr<bound_parameter_map_t> map) {
#ifdef DUCKDB_ALTERNATIVE_VERIFY
	// if alternate verification is enabled we run the original operator
	return;
#endif
	if (!op || !ClientConfig::GetConfig(context).verify_serializer) {
		return;
	}
	//! SELECT only for now
	if (!OperatorSupportsSerialization(*op)) {
		return;
	}

	// format (de)serialization of this operator
	try {
		MemoryStream stream;
		BinarySerializer::Serialize(*op, stream, true);
		stream.Rewind();
		bound_parameter_map_t parameters;
		auto new_plan = BinaryDeserializer::Deserialize<LogicalOperator>(stream, context, parameters);

		if (map) {
			*map = std::move(parameters);
		}
		op = std::move(new_plan);
	} catch (SerializationException &ex) {
		// pass
	} catch (NotImplementedException &ex) {
		// pass
	}
}

} // namespace duckdb
















namespace duckdb {

PragmaHandler::PragmaHandler(ClientContext &context) : context(context) {
}

void PragmaHandler::HandlePragmaStatementsInternal(vector<unique_ptr<SQLStatement>> &statements) {
	vector<unique_ptr<SQLStatement>> new_statements;
	for (idx_t i = 0; i < statements.size(); i++) {
		if (statements[i]->type == StatementType::MULTI_STATEMENT) {
			auto &multi_statement = statements[i]->Cast<MultiStatement>();
			for (auto &stmt : multi_statement.statements) {
				statements.push_back(std::move(stmt));
			}
			continue;
		}
		if (statements[i]->type == StatementType::PRAGMA_STATEMENT) {
			// PRAGMA statement: check if we need to replace it by a new set of statements
			PragmaHandler handler(context);
			string new_query;
			bool expanded = handler.HandlePragma(statements[i].get(), new_query);
			if (expanded) {
				// this PRAGMA statement gets replaced by a new query string
				// push the new query string through the parser again and add it to the transformer
				Parser parser(context.GetParserOptions());
				parser.ParseQuery(new_query);
				// insert the new statements and remove the old statement
				for (idx_t j = 0; j < parser.statements.size(); j++) {
					new_statements.push_back(std::move(parser.statements[j]));
				}
				continue;
			}
		}
		new_statements.push_back(std::move(statements[i]));
	}
	statements = std::move(new_statements);
}

void PragmaHandler::HandlePragmaStatements(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements) {
	// first check if there are any pragma statements
	bool found_pragma = false;
	for (idx_t i = 0; i < statements.size(); i++) {
		if (statements[i]->type == StatementType::PRAGMA_STATEMENT ||
		    statements[i]->type == StatementType::MULTI_STATEMENT) {
			found_pragma = true;
			break;
		}
	}
	if (!found_pragma) {
		// no pragmas: skip this step
		return;
	}
	context.RunFunctionInTransactionInternal(lock, [&]() { HandlePragmaStatementsInternal(statements); });
}

bool PragmaHandler::HandlePragma(SQLStatement *statement, string &resulting_query) { // PragmaInfo &info
	auto info = *(statement->Cast<PragmaStatement>()).info;
	auto &entry = Catalog::GetEntry<PragmaFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, info.name);
	string error;

	FunctionBinder function_binder(context);
	idx_t bound_idx = function_binder.BindFunction(entry.name, entry.functions, info, error);
	if (bound_idx == DConstants::INVALID_INDEX) {
		throw BinderException(error);
	}
	auto bound_function = entry.functions.GetFunctionByOffset(bound_idx);
	if (bound_function.query) {
		QueryErrorContext error_context(statement, statement->stmt_location);
		Binder::BindNamedParameters(bound_function.named_parameters, info.named_parameters, error_context,
		                            bound_function.name);
		FunctionParameters parameters {info.parameters, info.named_parameters};
		resulting_query = bound_function.query(context, parameters);
		return true;
	}
	return false;
}

} // namespace duckdb














namespace duckdb {

FlattenDependentJoins::FlattenDependentJoins(Binder &binder, const vector<CorrelatedColumnInfo> &correlated,
                                             bool perform_delim, bool any_join)
    : binder(binder), delim_offset(DConstants::INVALID_INDEX), correlated_columns(correlated),
      perform_delim(perform_delim), any_join(any_join) {
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		auto &col = correlated_columns[i];
		correlated_map[col.binding] = i;
		delim_types.push_back(col.type);
	}
}

bool FlattenDependentJoins::DetectCorrelatedExpressions(LogicalOperator *op, bool lateral, idx_t lateral_depth) {

	bool is_lateral_join = false;

	D_ASSERT(op);
	// check if this entry has correlated expressions
	if (op->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		is_lateral_join = true;
	}
	HasCorrelatedExpressions visitor(correlated_columns, lateral, lateral_depth);
	visitor.VisitOperator(*op);
	bool has_correlation = visitor.has_correlated_expressions;
	int child_idx = 0;
	// now visit the children of this entry and check if they have correlated expressions
	for (auto &child : op->children) {
		auto new_lateral_depth = lateral_depth;
		if (is_lateral_join && child_idx == 1) {
			new_lateral_depth = lateral_depth + 1;
		}
		// we OR the property with its children such that has_correlation is true if either
		// (1) this node has a correlated expression or
		// (2) one of its children has a correlated expression
		if (DetectCorrelatedExpressions(child.get(), lateral, new_lateral_depth)) {
			has_correlation = true;
		}
		child_idx++;
	}
	// set the entry in the map
	has_correlated_expressions[op] = has_correlation;
	return has_correlation;
}

unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoin(unique_ptr<LogicalOperator> plan) {
	bool propagate_null_values = true;
	auto result = PushDownDependentJoinInternal(std::move(plan), propagate_null_values, 0);
	if (!replacement_map.empty()) {
		// check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
		RewriteCountAggregates aggr(replacement_map);
		aggr.VisitOperator(*result);
	}
	return result;
}

bool SubqueryDependentFilter(Expression *expr) {
	if (expr->expression_class == ExpressionClass::BOUND_CONJUNCTION &&
	    expr->GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		auto &bound_conjuction = expr->Cast<BoundConjunctionExpression>();
		for (auto &child : bound_conjuction.children) {
			if (SubqueryDependentFilter(child.get())) {
				return true;
			}
		}
	}
	if (expr->expression_class == ExpressionClass::BOUND_SUBQUERY) {
		return true;
	}
	return false;
}

unique_ptr<LogicalOperator> FlattenDependentJoins::PushDownDependentJoinInternal(unique_ptr<LogicalOperator> plan,
                                                                                 bool &parent_propagate_null_values,
                                                                                 idx_t lateral_depth) {
	// first check if the logical operator has correlated expressions
	auto entry = has_correlated_expressions.find(plan.get());
	D_ASSERT(entry != has_correlated_expressions.end());
	if (!entry->second) {
		// we reached a node without correlated expressions
		// we can eliminate the dependent join now and create a simple cross product
		// now create the duplicate eliminated scan for this node
		auto left_columns = plan->GetColumnBindings().size();
		auto delim_index = binder.GenerateTableIndex();
		this->base_binding = ColumnBinding(delim_index, 0);
		this->delim_offset = left_columns;
		this->data_offset = 0;
		auto delim_scan = make_uniq<LogicalDelimGet>(delim_index, delim_types);
		return LogicalCrossProduct::Create(std::move(plan), std::move(delim_scan));
	}
	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_UNNEST:
	case LogicalOperatorType::LOGICAL_FILTER: {
		// filter
		// first we flatten the dependent join in the child of the filter
		for (auto &expr : plan->expressions) {
			any_join |= SubqueryDependentFilter(expr.get());
		}
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		// projection
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);

		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the projection list
		auto &proj = plan->Cast<LogicalProjection>();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(
			    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			plan->expressions.push_back(std::move(colref));
		}

		base_binding.table_index = proj.table_index;
		this->delim_offset = base_binding.column_index = plan->expressions.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &aggr = plan->Cast<LogicalAggregate>();
		// aggregate and group by
		// first we flatten the dependent join in the child of the projection
		for (auto &expr : plan->expressions) {
			parent_propagate_null_values &= expr->PropagatesNullValues();
		}
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the columns of the delim_scan to the grouping operators AND the projection list
		idx_t delim_table_index;
		idx_t delim_column_offset;
		idx_t delim_data_offset;
		auto new_group_count = perform_delim ? correlated_columns.size() : 1;
		for (idx_t i = 0; i < new_group_count; i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(
			    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			for (auto &set : aggr.grouping_sets) {
				set.insert(aggr.groups.size());
			}
			aggr.groups.push_back(std::move(colref));
		}
		if (!perform_delim) {
			// if we are not performing the duplicate elimination, we have only added the row_id column to the grouping
			// operators in this case, we push a FIRST aggregate for each of the remaining expressions
			delim_table_index = aggr.aggregate_index;
			delim_column_offset = aggr.expressions.size();
			delim_data_offset = aggr.groups.size();
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				auto &col = correlated_columns[i];
				auto first_aggregate = FirstFun::GetFunction(col.type);
				auto colref = make_uniq<BoundColumnRefExpression>(
				    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
				vector<unique_ptr<Expression>> aggr_children;
				aggr_children.push_back(std::move(colref));
				auto first_fun =
				    make_uniq<BoundAggregateExpression>(std::move(first_aggregate), std::move(aggr_children), nullptr,
				                                        nullptr, AggregateType::NON_DISTINCT);
				aggr.expressions.push_back(std::move(first_fun));
			}
		} else {
			delim_table_index = aggr.group_index;
			delim_column_offset = aggr.groups.size() - correlated_columns.size();
			delim_data_offset = aggr.groups.size();
		}
		if (aggr.groups.size() == new_group_count) {
			// we have to perform a LEFT OUTER JOIN between the result of this aggregate and the delim scan
			// FIXME: this does not always have to be a LEFT OUTER JOIN, depending on whether aggr.expressions return
			// NULL or a value
			unique_ptr<LogicalComparisonJoin> join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
			for (auto &aggr_exp : aggr.expressions) {
				auto &b_aggr_exp = aggr_exp->Cast<BoundAggregateExpression>();
				if (!b_aggr_exp.PropagatesNullValues() || any_join || !parent_propagate_null_values) {
					join = make_uniq<LogicalComparisonJoin>(JoinType::LEFT);
					break;
				}
			}
			auto left_index = binder.GenerateTableIndex();
			auto delim_scan = make_uniq<LogicalDelimGet>(left_index, delim_types);
			join->children.push_back(std::move(delim_scan));
			join->children.push_back(std::move(plan));
			for (idx_t i = 0; i < new_group_count; i++) {
				auto &col = correlated_columns[i];
				JoinCondition cond;
				cond.left = make_uniq<BoundColumnRefExpression>(col.name, col.type, ColumnBinding(left_index, i));
				cond.right = make_uniq<BoundColumnRefExpression>(
				    correlated_columns[i].type, ColumnBinding(delim_table_index, delim_column_offset + i));
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
				join->conditions.push_back(std::move(cond));
			}
			// for any COUNT aggregate we replace references to the column with: CASE WHEN COUNT(*) IS NULL THEN 0
			// ELSE COUNT(*) END
			for (idx_t i = 0; i < aggr.expressions.size(); i++) {
				D_ASSERT(aggr.expressions[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
				auto &bound = aggr.expressions[i]->Cast<BoundAggregateExpression>();
				vector<LogicalType> arguments;
				if (bound.function == CountFun::GetFunction() || bound.function == CountStarFun::GetFunction()) {
					// have to replace this ColumnBinding with the CASE expression
					replacement_map[ColumnBinding(aggr.aggregate_index, i)] = i;
				}
			}
			// now we update the delim_index
			base_binding.table_index = left_index;
			this->delim_offset = base_binding.column_index = 0;
			this->data_offset = 0;
			return std::move(join);
		} else {
			// update the delim_index
			base_binding.table_index = delim_table_index;
			this->delim_offset = base_binding.column_index = delim_column_offset;
			this->data_offset = delim_data_offset;
			return plan;
		}
	}
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		// cross product
		// push into both sides of the plan
		bool left_has_correlation = has_correlated_expressions.find(plan->children[0].get())->second;
		bool right_has_correlation = has_correlated_expressions.find(plan->children[1].get())->second;
		if (!right_has_correlation) {
			// only left has correlation: push into left
			plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
			                                                  parent_propagate_null_values, lateral_depth);
			return plan;
		}
		if (!left_has_correlation) {
			// only right has correlation: push into right
			plan->children[1] = PushDownDependentJoinInternal(std::move(plan->children[1]),
			                                                  parent_propagate_null_values, lateral_depth);
			return plan;
		}
		// both sides have correlation
		// turn into an inner join
		auto join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		auto left_binding = this->base_binding;
		plan->children[1] =
		    PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values, lateral_depth);
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			JoinCondition cond;
			cond.left = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			cond.right = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;
			join->conditions.push_back(std::move(cond));
		}
		join->children.push_back(std::move(plan->children[0]));
		join->children.push_back(std::move(plan->children[1]));
		return std::move(join);
	}
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN: {
		auto &dependent_join = plan->Cast<LogicalJoin>();
		if (!((dependent_join.join_type == JoinType::INNER) || (dependent_join.join_type == JoinType::LEFT))) {
			throw Exception("Dependent join can only be INNER or LEFT type");
		}
		D_ASSERT(plan->children.size() == 2);
		// Push all the bindings down to the left side so the right side knows where to refer DELIM_GET from
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);

		// Normal rewriter like in other joins
		RewriteCorrelatedExpressions rewriter(this->base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);

		// Recursive rewriter to visit right side of lateral join and update bindings from left
		RewriteCorrelatedExpressions recursive_rewriter(this->base_binding, correlated_map, lateral_depth + 1, true);
		recursive_rewriter.VisitOperator(*plan->children[1]);

		return plan;
	}
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		auto &join = plan->Cast<LogicalJoin>();
		D_ASSERT(plan->children.size() == 2);
		// check the correlated expressions in the children of the join
		bool left_has_correlation = has_correlated_expressions.find(plan->children[0].get())->second;
		bool right_has_correlation = has_correlated_expressions.find(plan->children[1].get())->second;

		if (join.join_type == JoinType::INNER) {
			// inner join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
				                                                  parent_propagate_null_values, lateral_depth);
				// Remove the correlated columns coming from outside for current join node
				return plan;
			}
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth);
				// Remove the correlated columns coming from outside for current join node
				return plan;
			}
		} else if (join.join_type == JoinType::LEFT) {
			// left outer join
			if (!right_has_correlation) {
				// only left has correlation: push into left
				plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
				                                                  parent_propagate_null_values, lateral_depth);
				// Remove the correlated columns coming from outside for current join node
				return plan;
			}
		} else if (join.join_type == JoinType::RIGHT) {
			// left outer join
			if (!left_has_correlation) {
				// only right has correlation: push into right
				plan->children[1] = PushDownDependentJoinInternal(std::move(plan->children[1]),
				                                                  parent_propagate_null_values, lateral_depth);
				return plan;
			}
		} else if (join.join_type == JoinType::MARK) {
			if (right_has_correlation) {
				throw Exception("MARK join with correlation in RHS not supported");
			}
			// push the child into the LHS
			plan->children[0] = PushDownDependentJoinInternal(std::move(plan->children[0]),
			                                                  parent_propagate_null_values, lateral_depth);
			// rewrite expressions in the join conditions
			RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
			rewriter.VisitOperator(*plan);
			return plan;
		} else {
			throw Exception("Unsupported join type for flattening correlated subquery");
		}
		// both sides have correlation
		// push into both sides
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		auto left_binding = this->base_binding;
		plan->children[1] =
		    PushDownDependentJoinInternal(std::move(plan->children[1]), parent_propagate_null_values, lateral_depth);
		auto right_binding = this->base_binding;
		// NOTE: for OUTER JOINS it matters what the BASE BINDING is after the join
		// for the LEFT OUTER JOIN, we want the LEFT side to be the base binding after we push
		// because the RIGHT binding might contain NULL values
		if (join.join_type == JoinType::LEFT) {
			this->base_binding = left_binding;
		} else if (join.join_type == JoinType::RIGHT) {
			this->base_binding = right_binding;
		}
		// add the correlated columns to the join conditions
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			auto left = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(left_binding.table_index, left_binding.column_index + i));
			auto right = make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(right_binding.table_index, right_binding.column_index + i));

			if (join.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			    join.type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
				JoinCondition cond;
				cond.left = std::move(left);
				cond.right = std::move(right);
				cond.comparison = ExpressionType::COMPARE_NOT_DISTINCT_FROM;

				auto &comparison_join = join.Cast<LogicalComparisonJoin>();
				comparison_join.conditions.push_back(std::move(cond));
			} else {
				auto &any_join = join.Cast<LogicalAnyJoin>();
				auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM,
				                                                       std::move(left), std::move(right));
				auto conjunction = make_uniq<BoundConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(comparison), std::move(any_join.condition));
				any_join.condition = std::move(conjunction);
			}
		}
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(right_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		return plan;
	}
	case LogicalOperatorType::LOGICAL_LIMIT: {
		auto &limit = plan->Cast<LogicalLimit>();
		if (limit.limit || limit.offset) {
			throw ParserException("Non-constant limit or offset not supported in correlated subquery");
		}
		auto rownum_alias = "limit_rownum";
		unique_ptr<LogicalOperator> child;
		unique_ptr<LogicalOrder> order_by;

		// check if the direct child of this LIMIT node is an ORDER BY node, if so, keep it separate
		// this is done for an optimization to avoid having to compute the total order
		if (plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
			order_by = unique_ptr_cast<LogicalOperator, LogicalOrder>(std::move(plan->children[0]));
			child = PushDownDependentJoinInternal(std::move(order_by->children[0]), parent_propagate_null_values,
			                                      lateral_depth);
		} else {
			child = PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values,
			                                      lateral_depth);
		}
		auto child_column_count = child->GetColumnBindings().size();
		// we push a row_number() OVER (PARTITION BY [correlated columns])
		auto window_index = binder.GenerateTableIndex();
		auto window = make_uniq<LogicalWindow>(window_index);
		auto row_number =
		    make_uniq<BoundWindowExpression>(ExpressionType::WINDOW_ROW_NUMBER, LogicalType::BIGINT, nullptr, nullptr);
		auto partition_count = perform_delim ? correlated_columns.size() : 1;
		for (idx_t i = 0; i < partition_count; i++) {
			auto &col = correlated_columns[i];
			auto colref = make_uniq<BoundColumnRefExpression>(
			    col.name, col.type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
			row_number->partitions.push_back(std::move(colref));
		}
		if (order_by) {
			// optimization: if there is an ORDER BY node followed by a LIMIT
			// rather than computing the entire order, we push the ORDER BY expressions into the row_num computation
			// this way, the order only needs to be computed per partition
			row_number->orders = std::move(order_by->orders);
		}
		row_number->start = WindowBoundary::UNBOUNDED_PRECEDING;
		row_number->end = WindowBoundary::CURRENT_ROW_ROWS;
		window->expressions.push_back(std::move(row_number));
		window->children.push_back(std::move(child));

		// add a filter based on the row_number
		// the filter we add is "row_number > offset AND row_number <= offset + limit"
		auto filter = make_uniq<LogicalFilter>();
		unique_ptr<Expression> condition;
		auto row_num_ref =
		    make_uniq<BoundColumnRefExpression>(rownum_alias, LogicalType::BIGINT, ColumnBinding(window_index, 0));

		int64_t upper_bound_limit = NumericLimits<int64_t>::Maximum();
		TryAddOperator::Operation(limit.offset_val, limit.limit_val, upper_bound_limit);
		auto upper_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(upper_bound_limit));
		condition = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, row_num_ref->Copy(),
		                                                 std::move(upper_bound));
		// we only need to add "row_number >= offset + 1" if offset is bigger than 0
		if (limit.offset_val > 0) {
			auto lower_bound = make_uniq<BoundConstantExpression>(Value::BIGINT(limit.offset_val));
			auto lower_comp = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN,
			                                                       row_num_ref->Copy(), std::move(lower_bound));
			auto conj = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(lower_comp),
			                                                  std::move(condition));
			condition = std::move(conj);
		}
		filter->expressions.push_back(std::move(condition));
		filter->children.push_back(std::move(window));
		// we prune away the row_number after the filter clause using the projection map
		for (idx_t i = 0; i < child_column_count; i++) {
			filter->projection_map.push_back(i);
		}
		return std::move(filter);
	}
	case LogicalOperatorType::LOGICAL_LIMIT_PERCENT: {
		// NOTE: limit percent could be supported in a manner similar to the LIMIT above
		// but instead of filtering by an exact number of rows, the limit should be expressed as
		// COUNT computed over the partition multiplied by the percentage
		throw ParserException("Limit percent operator not supported in correlated subquery");
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		auto &window = plan->Cast<LogicalWindow>();
		// push into children
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		// add the correlated columns to the PARTITION BY clauses in the Window
		for (auto &expr : window.expressions) {
			D_ASSERT(expr->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
			auto &w = expr->Cast<BoundWindowExpression>();
			for (idx_t i = 0; i < correlated_columns.size(); i++) {
				w.partitions.push_back(make_uniq<BoundColumnRefExpression>(
				    correlated_columns[i].type,
				    ColumnBinding(base_binding.table_index, base_binding.column_index + i)));
			}
		}
		return plan;
	}
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT:
	case LogicalOperatorType::LOGICAL_UNION: {
		auto &setop = plan->Cast<LogicalSetOperation>();
		// set operator, push into both children
#ifdef DEBUG
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
		D_ASSERT(plan->children[0]->types == plan->children[1]->types);
#endif
		plan->children[0] = PushDownDependentJoin(std::move(plan->children[0]));
		plan->children[1] = PushDownDependentJoin(std::move(plan->children[1]));
#ifdef DEBUG
		D_ASSERT(plan->children[0]->GetColumnBindings().size() == plan->children[1]->GetColumnBindings().size());
		plan->children[0]->ResolveOperatorTypes();
		plan->children[1]->ResolveOperatorTypes();
		D_ASSERT(plan->children[0]->types == plan->children[1]->types);
#endif
		// we have to refer to the setop index now
		base_binding.table_index = setop.table_index;
		base_binding.column_index = setop.column_count;
		setop.column_count += correlated_columns.size();
		return plan;
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		auto &distinct = plan->Cast<LogicalDistinct>();
		// push down into child
		distinct.children[0] = PushDownDependentJoin(std::move(distinct.children[0]));
		// add all correlated columns to the distinct targets
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			distinct.distinct_targets.push_back(make_uniq<BoundColumnRefExpression>(
			    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i)));
		}
		return plan;
	}
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET: {
		// expression get
		// first we flatten the dependent join in the child
		plan->children[0] =
		    PushDownDependentJoinInternal(std::move(plan->children[0]), parent_propagate_null_values, lateral_depth);
		// then we replace any correlated expressions with the corresponding entry in the correlated_map
		RewriteCorrelatedExpressions rewriter(base_binding, correlated_map, lateral_depth);
		rewriter.VisitOperator(*plan);
		// now we add all the correlated columns to each of the expressions of the expression scan
		auto &expr_get = plan->Cast<LogicalExpressionGet>();
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			for (auto &expr_list : expr_get.expressions) {
				auto colref = make_uniq<BoundColumnRefExpression>(
				    correlated_columns[i].type, ColumnBinding(base_binding.table_index, base_binding.column_index + i));
				expr_list.push_back(std::move(colref));
			}
			expr_get.expr_types.push_back(correlated_columns[i].type);
		}

		base_binding.table_index = expr_get.table_index;
		this->delim_offset = base_binding.column_index = expr_get.expr_types.size() - correlated_columns.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_PIVOT:
		throw BinderException("PIVOT is not supported in correlated subqueries yet");
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		plan->children[0] = PushDownDependentJoin(std::move(plan->children[0]));
		return plan;
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = plan->Cast<LogicalGet>();
		if (get.children.size() != 1) {
			throw InternalException("Flatten dependent joins - logical get encountered without children");
		}
		plan->children[0] = PushDownDependentJoin(std::move(plan->children[0]));
		for (idx_t i = 0; i < correlated_columns.size(); i++) {
			get.projected_input.push_back(this->delim_offset + i);
		}
		this->delim_offset = get.returned_types.size();
		this->data_offset = 0;
		return plan;
	}
	case LogicalOperatorType::LOGICAL_RECURSIVE_CTE: {
		throw BinderException("Recursive CTEs not (yet) supported in correlated subquery");
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		throw BinderException("Materialized CTEs not (yet) supported in correlated subquery");
	}
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: {
		throw BinderException("Nested lateral joins or lateral joins in correlated subqueries are not (yet) supported");
	}
	case LogicalOperatorType::LOGICAL_SAMPLE:
		throw BinderException("Sampling in correlated subqueries is not (yet) supported");
	default:
		throw InternalException("Logical operator type \"%s\" for dependent join", LogicalOperatorToString(plan->type));
	}
}

} // namespace duckdb





#include <algorithm>

namespace duckdb {

HasCorrelatedExpressions::HasCorrelatedExpressions(const vector<CorrelatedColumnInfo> &correlated, bool lateral,
                                                   idx_t lateral_depth)
    : has_correlated_expressions(false), lateral(lateral), correlated_columns(correlated),
      lateral_depth(lateral_depth) {
}

void HasCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> HasCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	// Indicates local correlations (all correlations within a child) for the root
	if (expr.depth <= lateral_depth) {
		return nullptr;
	}

	// Should never happen
	if (expr.depth > 1 + lateral_depth) {
		if (lateral) {
			throw BinderException("Invalid lateral depth encountered for an expression");
		}
		throw InternalException("Expression with depth > 1 detected in non-lateral join");
	}
	// Note: This is added, since we only want to set has_correlated_expressions to true when the
	// BoundSubqueryExpression has the same bindings as one of the correlated_columns from the left hand side
	// (correlated_columns is the correlated_columns from left hand side)
	bool found_match = false;
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		if (correlated_columns[i].binding == expr.binding) {
			found_match = true;
			break;
		}
	}
	// correlated column reference
	D_ASSERT(expr.depth == lateral_depth + 1);
	has_correlated_expressions = found_match;
	return nullptr;
}

unique_ptr<Expression> HasCorrelatedExpressions::VisitReplace(BoundSubqueryExpression &expr,
                                                              unique_ptr<Expression> *expr_ptr) {
	if (!expr.IsCorrelated()) {
		return nullptr;
	}
	// check if the subquery contains any of the correlated expressions that we are concerned about in this node
	for (idx_t i = 0; i < correlated_columns.size(); i++) {
		if (std::find(expr.binder->correlated_columns.begin(), expr.binder->correlated_columns.end(),
		              correlated_columns[i]) != expr.binder->correlated_columns.end()) {
			has_correlated_expressions = true;
			break;
		}
	}
	return nullptr;
}

} // namespace duckdb












namespace duckdb {

RewriteCorrelatedExpressions::RewriteCorrelatedExpressions(ColumnBinding base_binding,
                                                           column_binding_map_t<idx_t> &correlated_map,
                                                           idx_t lateral_depth, bool recursive_rewrite)
    : base_binding(base_binding), correlated_map(correlated_map), lateral_depth(lateral_depth),
      recursive_rewrite(recursive_rewrite) {
}

void RewriteCorrelatedExpressions::VisitOperator(LogicalOperator &op) {
	if (recursive_rewrite) {
		// Update column bindings from left child of lateral to right child
		if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
			D_ASSERT(op.children.size() == 2);
			VisitOperator(*op.children[0]);
			lateral_depth++;
			VisitOperator(*op.children[1]);
			lateral_depth--;
		} else {
			VisitOperatorChildren(op);
		}
	}
	// update the bindings in the correlated columns of the dependendent join
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &plan = op.Cast<LogicalDependentJoin>();
		for (auto &corr : plan.correlated_columns) {
			auto entry = correlated_map.find(corr.binding);
			if (entry != correlated_map.end()) {
				corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			}
		}
	}
	VisitOperatorExpressions(op);
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundColumnRefExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (expr.depth <= lateral_depth) {
		// Indicates local correlations not relevant for the current the rewrite
		return nullptr;
	}
	// correlated column reference
	// replace with the entry referring to the duplicate eliminated scan
	// if this assertion occurs it generally means the bindings are inappropriate set in the binder or
	// we either missed to account for lateral binder or over-counted for the lateral binder
	D_ASSERT(expr.depth == 1 + lateral_depth);
	auto entry = correlated_map.find(expr.binding);
	D_ASSERT(entry != correlated_map.end());

	expr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
	if (recursive_rewrite) {
		D_ASSERT(expr.depth > 1);
		expr.depth--;
	} else {
		expr.depth = 0;
	}
	return nullptr;
}

unique_ptr<Expression> RewriteCorrelatedExpressions::VisitReplace(BoundSubqueryExpression &expr,
                                                                  unique_ptr<Expression> *expr_ptr) {
	if (!expr.IsCorrelated()) {
		return nullptr;
	}
	// subquery detected within this subquery
	// recursively rewrite it using the RewriteCorrelatedRecursive class
	RewriteCorrelatedRecursive rewrite(expr, base_binding, correlated_map);
	rewrite.RewriteCorrelatedSubquery(expr);
	return nullptr;
}

RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedRecursive(
    BoundSubqueryExpression &parent, ColumnBinding base_binding, column_binding_map_t<idx_t> &correlated_map)
    : parent(parent), base_binding(base_binding), correlated_map(correlated_map) {
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteJoinRefRecursive(BoundTableRef &ref) {
	// recursively rewrite bindings in the correlated columns for the table ref and all the children
	if (ref.type == TableReferenceType::JOIN) {
		auto &bound_join = ref.Cast<BoundJoinRef>();
		for (auto &corr : bound_join.correlated_columns) {
			auto entry = correlated_map.find(corr.binding);
			if (entry != correlated_map.end()) {
				corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			}
		}
		RewriteJoinRefRecursive(*bound_join.left);
		RewriteJoinRefRecursive(*bound_join.right);
	}
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedSubquery(
    BoundSubqueryExpression &expr) {
	// rewrite the binding in the correlated list of the subquery)
	for (auto &corr : expr.binder->correlated_columns) {
		auto entry = correlated_map.find(corr.binding);
		if (entry != correlated_map.end()) {
			corr.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
		}
	}
	// TODO: Cleanup and find a better way to do this
	auto &node = *expr.subquery;
	if (node.type == QueryNodeType::SELECT_NODE) {
		// Found an unplanned select node, need to update column bindings correlated columns in the from tables
		auto &bound_select = node.Cast<BoundSelectNode>();
		if (bound_select.from_table) {
			BoundTableRef &table_ref = *bound_select.from_table;
			RewriteJoinRefRecursive(table_ref);
		}
	}
	// now rewrite any correlated BoundColumnRef expressions inside the subquery
	ExpressionIterator::EnumerateQueryNodeChildren(*expr.subquery,
	                                               [&](Expression &child) { RewriteCorrelatedExpressions(child); });
}

void RewriteCorrelatedExpressions::RewriteCorrelatedRecursive::RewriteCorrelatedExpressions(Expression &child) {
	if (child.type == ExpressionType::BOUND_COLUMN_REF) {
		// bound column reference
		auto &bound_colref = child.Cast<BoundColumnRefExpression>();
		if (bound_colref.depth == 0) {
			// not a correlated column, ignore
			return;
		}
		// correlated column
		// check the correlated map
		auto entry = correlated_map.find(bound_colref.binding);
		if (entry != correlated_map.end()) {
			// we found the column in the correlated map!
			// update the binding and reduce the depth by 1
			bound_colref.binding = ColumnBinding(base_binding.table_index, base_binding.column_index + entry->second);
			bound_colref.depth--;
		}
	} else if (child.type == ExpressionType::SUBQUERY) {
		// we encountered another subquery: rewrite recursively
		D_ASSERT(child.GetExpressionClass() == ExpressionClass::BOUND_SUBQUERY);
		auto &bound_subquery = child.Cast<BoundSubqueryExpression>();
		RewriteCorrelatedRecursive rewrite(bound_subquery, base_binding, correlated_map);
		rewrite.RewriteCorrelatedSubquery(bound_subquery);
	}
}

RewriteCountAggregates::RewriteCountAggregates(column_binding_map_t<idx_t> &replacement_map)
    : replacement_map(replacement_map) {
}

unique_ptr<Expression> RewriteCountAggregates::VisitReplace(BoundColumnRefExpression &expr,
                                                            unique_ptr<Expression> *expr_ptr) {
	auto entry = replacement_map.find(expr.binding);
	if (entry != replacement_map.end()) {
		// reference to a COUNT(*) aggregate
		// replace this with CASE WHEN COUNT(*) IS NULL THEN 0 ELSE COUNT(*) END
		auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		is_null->children.push_back(expr.Copy());
		auto check = std::move(is_null);
		auto result_if_true = make_uniq<BoundConstantExpression>(Value::Numeric(expr.return_type, 0));
		auto result_if_false = std::move(*expr_ptr);
		return make_uniq<BoundCaseExpression>(std::move(check), std::move(result_if_true), std::move(result_if_false));
	}
	return nullptr;
}

} // namespace duckdb













#include <algorithm>

namespace duckdb {

Binding::Binding(BindingType binding_type, const string &alias, vector<LogicalType> coltypes, vector<string> colnames,
                 idx_t index)
    : binding_type(binding_type), alias(alias), index(index), types(std::move(coltypes)), names(std::move(colnames)) {
	D_ASSERT(types.size() == names.size());
	for (idx_t i = 0; i < names.size(); i++) {
		auto &name = names[i];
		D_ASSERT(!name.empty());
		if (name_map.find(name) != name_map.end()) {
			throw BinderException("table \"%s\" has duplicate column name \"%s\"", alias, name);
		}
		name_map[name] = i;
	}
}

bool Binding::TryGetBindingIndex(const string &column_name, column_t &result) {
	auto entry = name_map.find(column_name);
	if (entry == name_map.end()) {
		return false;
	}
	auto column_info = entry->second;
	result = column_info;
	return true;
}

column_t Binding::GetBindingIndex(const string &column_name) {
	column_t result;
	if (!TryGetBindingIndex(column_name, result)) {
		throw InternalException("Binding index for column \"%s\" not found", column_name);
	}
	return result;
}

bool Binding::HasMatchingBinding(const string &column_name) {
	column_t result;
	return TryGetBindingIndex(column_name, result);
}

string Binding::ColumnNotFoundError(const string &column_name) const {
	return StringUtil::Format("Values list \"%s\" does not have a column named \"%s\"", alias, column_name);
}

BindResult Binding::Bind(ColumnRefExpression &colref, idx_t depth) {
	column_t column_index;
	bool success = false;
	success = TryGetBindingIndex(colref.GetColumnName(), column_index);
	if (!success) {
		return BindResult(ColumnNotFoundError(colref.GetColumnName()));
	}
	ColumnBinding binding;
	binding.table_index = index;
	binding.column_index = column_index;
	LogicalType sql_type = types[column_index];
	if (colref.alias.empty()) {
		colref.alias = names[column_index];
	}
	return BindResult(make_uniq<BoundColumnRefExpression>(colref.GetName(), sql_type, binding, depth));
}

optional_ptr<StandardEntry> Binding::GetStandardEntry() {
	return nullptr;
}

EntryBinding::EntryBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p, idx_t index,
                           StandardEntry &entry)
    : Binding(BindingType::CATALOG_ENTRY, alias, std::move(types_p), std::move(names_p), index), entry(entry) {
}

optional_ptr<StandardEntry> EntryBinding::GetStandardEntry() {
	return &entry;
}

TableBinding::TableBinding(const string &alias, vector<LogicalType> types_p, vector<string> names_p,
                           vector<column_t> &bound_column_ids, optional_ptr<StandardEntry> entry, idx_t index,
                           bool add_row_id)
    : Binding(BindingType::TABLE, alias, std::move(types_p), std::move(names_p), index),
      bound_column_ids(bound_column_ids), entry(entry) {
	if (add_row_id) {
		if (name_map.find("rowid") == name_map.end()) {
			name_map["rowid"] = COLUMN_IDENTIFIER_ROW_ID;
		}
	}
}

static void ReplaceAliases(ParsedExpression &expr, const ColumnList &list,
                           const unordered_map<idx_t, string> &alias_map) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		D_ASSERT(!colref.IsQualified());
		auto &col_names = colref.column_names;
		D_ASSERT(col_names.size() == 1);
		auto idx_entry = list.GetColumnIndex(col_names[0]);
		auto &alias = alias_map.at(idx_entry.index);
		col_names = {alias};
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { ReplaceAliases((ParsedExpression &)child, list, alias_map); });
}

static void BakeTableName(ParsedExpression &expr, const string &table_name) {
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		D_ASSERT(!colref.IsQualified());
		auto &col_names = colref.column_names;
		col_names.insert(col_names.begin(), table_name);
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expr, [&](const ParsedExpression &child) { BakeTableName((ParsedExpression &)child, table_name); });
}

unique_ptr<ParsedExpression> TableBinding::ExpandGeneratedColumn(const string &column_name) {
	auto catalog_entry = GetStandardEntry();
	D_ASSERT(catalog_entry); // Should only be called on a TableBinding

	D_ASSERT(catalog_entry->type == CatalogType::TABLE_ENTRY);
	auto &table_entry = catalog_entry->Cast<TableCatalogEntry>();

	// Get the index of the generated column
	auto column_index = GetBindingIndex(column_name);
	D_ASSERT(table_entry.GetColumn(LogicalIndex(column_index)).Generated());
	// Get a copy of the generated column
	auto expression = table_entry.GetColumn(LogicalIndex(column_index)).GeneratedExpression().Copy();
	unordered_map<idx_t, string> alias_map;
	for (auto &entry : name_map) {
		alias_map[entry.second] = entry.first;
	}
	ReplaceAliases(*expression, table_entry.GetColumns(), alias_map);
	BakeTableName(*expression, alias);
	return (expression);
}

const vector<column_t> &TableBinding::GetBoundColumnIds() const {
#ifdef DEBUG
	unordered_set<column_t> column_ids;
	for (auto &id : bound_column_ids) {
		auto result = column_ids.insert(id);
		// assert that all entries in the bound_column_ids are unique
		D_ASSERT(result.second);
		auto it = std::find_if(name_map.begin(), name_map.end(),
		                       [&](const std::pair<const string, column_t> &it) { return it.second == id; });
		// assert that every id appears in the name_map
		D_ASSERT(it != name_map.end());
		// the order that they appear in is not guaranteed to be sequential
	}
#endif
	return bound_column_ids;
}

ColumnBinding TableBinding::GetColumnBinding(column_t column_index) {
	auto &column_ids = bound_column_ids;
	ColumnBinding binding;

	// Locate the column_id that matches the 'column_index'
	auto it = std::find_if(column_ids.begin(), column_ids.end(),
	                       [&](const column_t &id) -> bool { return id == column_index; });
	// Get the index of it
	binding.column_index = std::distance(column_ids.begin(), it);
	// If it wasn't found, add it
	if (it == column_ids.end()) {
		column_ids.push_back(column_index);
	}

	binding.table_index = index;
	return binding;
}

BindResult TableBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	auto &column_name = colref.GetColumnName();
	column_t column_index;
	bool success = false;
	success = TryGetBindingIndex(column_name, column_index);
	if (!success) {
		return BindResult(ColumnNotFoundError(column_name));
	}
	auto entry = GetStandardEntry();
	if (entry && column_index != COLUMN_IDENTIFIER_ROW_ID) {
		D_ASSERT(entry->type == CatalogType::TABLE_ENTRY);
		// Either there is no table, or the columns category has to be standard
		auto &table_entry = entry->Cast<TableCatalogEntry>();
		auto &column_entry = table_entry.GetColumn(LogicalIndex(column_index));
		(void)table_entry;
		(void)column_entry;
		D_ASSERT(column_entry.Category() == TableColumnType::STANDARD);
	}
	// fetch the type of the column
	LogicalType col_type;
	if (column_index == COLUMN_IDENTIFIER_ROW_ID) {
		// row id: BIGINT type
		col_type = LogicalType::BIGINT;
	} else {
		// normal column: fetch type from base column
		col_type = types[column_index];
		if (colref.alias.empty()) {
			colref.alias = names[column_index];
		}
	}
	ColumnBinding binding = GetColumnBinding(column_index);
	return BindResult(make_uniq<BoundColumnRefExpression>(colref.GetName(), col_type, binding, depth));
}

optional_ptr<StandardEntry> TableBinding::GetStandardEntry() {
	return entry;
}

string TableBinding::ColumnNotFoundError(const string &column_name) const {
	return StringUtil::Format("Table \"%s\" does not have a column named \"%s\"", alias, column_name);
}

DummyBinding::DummyBinding(vector<LogicalType> types_p, vector<string> names_p, string dummy_name_p)
    : Binding(BindingType::DUMMY, DummyBinding::DUMMY_NAME + dummy_name_p, std::move(types_p), std::move(names_p),
              DConstants::INVALID_INDEX),
      dummy_name(std::move(dummy_name_p)) {
}

BindResult DummyBinding::Bind(ColumnRefExpression &colref, idx_t depth) {
	column_t column_index;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_index)) {
		throw InternalException("Column %s not found in bindings", colref.GetColumnName());
	}
	ColumnBinding binding(index, column_index);

	// we are binding a parameter to create the dummy binding, no arguments are supplied
	return BindResult(make_uniq<BoundColumnRefExpression>(colref.GetName(), types[column_index], binding, depth));
}

BindResult DummyBinding::Bind(ColumnRefExpression &colref, idx_t lambda_index, idx_t depth) {
	column_t column_index;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_index)) {
		throw InternalException("Column %s not found in bindings", colref.GetColumnName());
	}
	ColumnBinding binding(index, column_index);
	return BindResult(
	    make_uniq<BoundLambdaRefExpression>(colref.GetName(), types[column_index], binding, lambda_index, depth));
}

unique_ptr<ParsedExpression> DummyBinding::ParamToArg(ColumnRefExpression &colref) {
	column_t column_index;
	if (!TryGetBindingIndex(colref.GetColumnName(), column_index)) {
		throw InternalException("Column %s not found in macro", colref.GetColumnName());
	}
	auto arg = (*arguments)[column_index]->Copy();
	arg->alias = colref.alias;
	return arg;
}

} // namespace duckdb





namespace duckdb {

void TableFilterSet::PushFilter(idx_t column_index, unique_ptr<TableFilter> filter) {
	auto entry = filters.find(column_index);
	if (entry == filters.end()) {
		// no filter yet: push the filter directly
		filters[column_index] = std::move(filter);
	} else {
		// there is already a filter: AND it together
		if (entry->second->filter_type == TableFilterType::CONJUNCTION_AND) {
			auto &and_filter = entry->second->Cast<ConjunctionAndFilter>();
			and_filter.child_filters.push_back(std::move(filter));
		} else {
			auto and_filter = make_uniq<ConjunctionAndFilter>();
			and_filter->child_filters.push_back(std::move(entry->second));
			and_filter->child_filters.push_back(std::move(filter));
			filters[column_index] = std::move(and_filter);
		}
	}
}

} // namespace duckdb




namespace duckdb {

//===--------------------------------------------------------------------===//
// Arena Chunk
//===--------------------------------------------------------------------===//
ArenaChunk::ArenaChunk(Allocator &allocator, idx_t size) : current_position(0), maximum_size(size), prev(nullptr) {
	D_ASSERT(size > 0);
	data = allocator.Allocate(size);
}
ArenaChunk::~ArenaChunk() {
	if (next) {
		auto current_next = std::move(next);
		while (current_next) {
			current_next = std::move(current_next->next);
		}
	}
}

//===--------------------------------------------------------------------===//
// Allocator Wrapper
//===--------------------------------------------------------------------===//
struct ArenaAllocatorData : public PrivateAllocatorData {
	explicit ArenaAllocatorData(ArenaAllocator &allocator) : allocator(allocator) {
	}

	ArenaAllocator &allocator;
};

static data_ptr_t ArenaAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto &allocator_data = private_data->Cast<ArenaAllocatorData>();
	return allocator_data.allocator.Allocate(size);
}

static void ArenaAllocatorFree(PrivateAllocatorData *, data_ptr_t, idx_t) {
	// nop
}

static data_ptr_t ArenaAllocateReallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                          idx_t size) {
	auto &allocator_data = private_data->Cast<ArenaAllocatorData>();
	return allocator_data.allocator.Reallocate(pointer, old_size, size);
}
//===--------------------------------------------------------------------===//
// Arena Allocator
//===--------------------------------------------------------------------===//
ArenaAllocator::ArenaAllocator(Allocator &allocator, idx_t initial_capacity)
    : allocator(allocator), arena_allocator(ArenaAllocatorAllocate, ArenaAllocatorFree, ArenaAllocateReallocate,
                                            make_uniq<ArenaAllocatorData>(*this)) {
	head = nullptr;
	tail = nullptr;
	current_capacity = initial_capacity;
}

ArenaAllocator::~ArenaAllocator() {
}

data_ptr_t ArenaAllocator::Allocate(idx_t len) {
	D_ASSERT(!head || head->current_position <= head->maximum_size);
	if (!head || head->current_position + len > head->maximum_size) {
		do {
			current_capacity *= 2;
		} while (current_capacity < len);
		auto new_chunk = make_unsafe_uniq<ArenaChunk>(allocator, current_capacity);
		if (head) {
			head->prev = new_chunk.get();
			new_chunk->next = std::move(head);
		} else {
			tail = new_chunk.get();
		}
		head = std::move(new_chunk);
	}
	D_ASSERT(head->current_position + len <= head->maximum_size);
	auto result = head->data.get() + head->current_position;
	head->current_position += len;
	return result;
}

data_ptr_t ArenaAllocator::Reallocate(data_ptr_t pointer, idx_t old_size, idx_t size) {
	D_ASSERT(head);
	if (old_size == size) {
		// nothing to do
		return pointer;
	}

	auto head_ptr = head->data.get() + head->current_position;
	int64_t diff = size - old_size;
	if (pointer == head_ptr && (size < old_size || head->current_position + diff <= head->maximum_size)) {
		// passed pointer is the head pointer, and the diff fits on the current chunk
		head->current_position += diff;
		return pointer;
	} else {
		// allocate new memory
		auto result = Allocate(size);
		memcpy(result, pointer, old_size);
		return result;
	}
}

data_ptr_t ArenaAllocator::AllocateAligned(idx_t size) {
	return Allocate(AlignValue<idx_t>(size));
}

data_ptr_t ArenaAllocator::ReallocateAligned(data_ptr_t pointer, idx_t old_size, idx_t size) {
	return Reallocate(pointer, old_size, AlignValue<idx_t>(size));
}

void ArenaAllocator::Reset() {
	if (head) {
		// destroy all chunks except the current one
		if (head->next) {
			auto current_next = std::move(head->next);
			while (current_next) {
				current_next = std::move(current_next->next);
			}
		}
		tail = head.get();

		// reset the head
		head->current_position = 0;
		head->prev = nullptr;
	}
}

void ArenaAllocator::Destroy() {
	head = nullptr;
	tail = nullptr;
	current_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY;
}

void ArenaAllocator::Move(ArenaAllocator &other) {
	D_ASSERT(!other.head);
	other.tail = tail;
	other.head = std::move(head);
	other.current_capacity = current_capacity;
	Destroy();
}

ArenaChunk *ArenaAllocator::GetHead() {
	return head.get();
}

ArenaChunk *ArenaAllocator::GetTail() {
	return tail;
}

bool ArenaAllocator::IsEmpty() const {
	return head == nullptr;
}

idx_t ArenaAllocator::SizeInBytes() const {
	idx_t total_size = 0;
	if (!IsEmpty()) {
		auto current = head.get();
		while (current != nullptr) {
			total_size += current->current_position;
			current = current->next.get();
		}
	}
	return total_size;
}

} // namespace duckdb



namespace duckdb {

Block::Block(Allocator &allocator, block_id_t id)
    : FileBuffer(allocator, FileBufferType::BLOCK, Storage::BLOCK_SIZE), id(id) {
}

Block::Block(Allocator &allocator, block_id_t id, uint32_t internal_size)
    : FileBuffer(allocator, FileBufferType::BLOCK, internal_size), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

Block::Block(FileBuffer &source, block_id_t id) : FileBuffer(source, FileBufferType::BLOCK), id(id) {
	D_ASSERT((AllocSize() & (Storage::SECTOR_SIZE - 1)) == 0);
}

} // namespace duckdb








namespace duckdb {

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p)
    : block_manager(block_manager), readers(0), block_id(block_id_p), buffer(nullptr), eviction_timestamp(0),
      can_destroy(false), memory_charge(block_manager.buffer_manager.GetBufferPool()), unswizzled(nullptr) {
	eviction_timestamp = 0;
	state = BlockState::BLOCK_UNLOADED;
	memory_usage = Storage::BLOCK_ALLOC_SIZE;
}

BlockHandle::BlockHandle(BlockManager &block_manager, block_id_t block_id_p, unique_ptr<FileBuffer> buffer_p,
                         bool can_destroy_p, idx_t block_size, BufferPoolReservation &&reservation)
    : block_manager(block_manager), readers(0), block_id(block_id_p), eviction_timestamp(0), can_destroy(can_destroy_p),
      memory_charge(block_manager.buffer_manager.GetBufferPool()), unswizzled(nullptr) {
	buffer = std::move(buffer_p);
	state = BlockState::BLOCK_LOADED;
	memory_usage = block_size;
	memory_charge = std::move(reservation);
}

BlockHandle::~BlockHandle() { // NOLINT: allow internal exceptions
	// being destroyed, so any unswizzled pointers are just binary junk now.
	unswizzled = nullptr;
	auto &buffer_manager = block_manager.buffer_manager;
	// no references remain to this block: erase
	if (buffer && state == BlockState::BLOCK_LOADED) {
		D_ASSERT(memory_charge.size > 0);
		// the block is still loaded in memory: erase it
		buffer.reset();
		memory_charge.Resize(0);
	} else {
		D_ASSERT(memory_charge.size == 0);
	}
	buffer_manager.GetBufferPool().PurgeQueue();
	block_manager.UnregisterBlock(block_id, can_destroy);
}

unique_ptr<Block> AllocateBlock(BlockManager &block_manager, unique_ptr<FileBuffer> reusable_buffer,
                                block_id_t block_id) {
	if (reusable_buffer) {
		// re-usable buffer: re-use it
		if (reusable_buffer->type == FileBufferType::BLOCK) {
			// we can reuse the buffer entirely
			auto &block = reinterpret_cast<Block &>(*reusable_buffer);
			block.id = block_id;
			return unique_ptr_cast<FileBuffer, Block>(std::move(reusable_buffer));
		}
		auto block = block_manager.CreateBlock(block_id, reusable_buffer.get());
		reusable_buffer.reset();
		return block;
	} else {
		// no re-usable buffer: allocate a new block
		return block_manager.CreateBlock(block_id, nullptr);
	}
}

BufferHandle BlockHandle::Load(shared_ptr<BlockHandle> &handle, unique_ptr<FileBuffer> reusable_buffer) {
	if (handle->state == BlockState::BLOCK_LOADED) {
		// already loaded
		D_ASSERT(handle->buffer);
		return BufferHandle(handle, handle->buffer.get());
	}

	auto &block_manager = handle->block_manager;
	if (handle->block_id < MAXIMUM_BLOCK) {
		auto block = AllocateBlock(block_manager, std::move(reusable_buffer), handle->block_id);
		block_manager.Read(*block);
		handle->buffer = std::move(block);
	} else {
		if (handle->can_destroy) {
			return BufferHandle();
		} else {
			handle->buffer =
			    block_manager.buffer_manager.ReadTemporaryBuffer(handle->block_id, std::move(reusable_buffer));
		}
	}
	handle->state = BlockState::BLOCK_LOADED;
	return BufferHandle(handle, handle->buffer.get());
}

unique_ptr<FileBuffer> BlockHandle::UnloadAndTakeBlock() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded: nothing to do
		return nullptr;
	}
	D_ASSERT(!unswizzled);
	D_ASSERT(CanUnload());

	if (block_id >= MAXIMUM_BLOCK && !can_destroy) {
		// temporary block that cannot be destroyed: write to temporary file
		block_manager.buffer_manager.WriteTemporaryBuffer(block_id, *buffer);
	}
	memory_charge.Resize(0);
	state = BlockState::BLOCK_UNLOADED;
	return std::move(buffer);
}

void BlockHandle::Unload() {
	auto block = UnloadAndTakeBlock();
	block.reset();
}

bool BlockHandle::CanUnload() {
	if (state == BlockState::BLOCK_UNLOADED) {
		// already unloaded
		return false;
	}
	if (readers > 0) {
		// there are active readers
		return false;
	}
	if (block_id >= MAXIMUM_BLOCK && !can_destroy && !block_manager.buffer_manager.HasTemporaryDirectory()) {
		// in order to unload this block we need to write it to a temporary buffer
		// however, no temporary directory is specified!
		// hence we cannot unload the block
		return false;
	}
	return true;
}

} // namespace duckdb






namespace duckdb {

BlockManager::BlockManager(BufferManager &buffer_manager)
    : buffer_manager(buffer_manager), metadata_manager(make_uniq<MetadataManager>(*this, buffer_manager)) {
}

shared_ptr<BlockHandle> BlockManager::RegisterBlock(block_id_t block_id) {
	lock_guard<mutex> lock(blocks_lock);
	// check if the block already exists
	auto entry = blocks.find(block_id);
	if (entry != blocks.end()) {
		// already exists: check if it hasn't expired yet
		auto existing_ptr = entry->second.lock();
		if (existing_ptr) {
			//! it hasn't! return it
			return existing_ptr;
		}
	}
	// create a new block pointer for this block
	auto result = make_shared<BlockHandle>(*this, block_id);
	// register the block pointer in the set of blocks as a weak pointer
	blocks[block_id] = weak_ptr<BlockHandle>(result);
	return result;
}

shared_ptr<BlockHandle> BlockManager::ConvertToPersistent(block_id_t block_id, shared_ptr<BlockHandle> old_block) {
	// pin the old block to ensure we have it loaded in memory
	auto old_handle = buffer_manager.Pin(old_block);
	D_ASSERT(old_block->state == BlockState::BLOCK_LOADED);
	D_ASSERT(old_block->buffer);

	// Temp buffers can be larger than the storage block size. But persistent buffers
	// cannot.
	D_ASSERT(old_block->buffer->AllocSize() <= Storage::BLOCK_ALLOC_SIZE);

	// register a block with the new block id
	auto new_block = RegisterBlock(block_id);
	D_ASSERT(new_block->state == BlockState::BLOCK_UNLOADED);
	D_ASSERT(new_block->readers == 0);

	// move the data from the old block into data for the new block
	new_block->state = BlockState::BLOCK_LOADED;
	new_block->buffer = ConvertBlock(block_id, *old_block->buffer);
	new_block->memory_usage = old_block->memory_usage;
	new_block->memory_charge = std::move(old_block->memory_charge);

	// clear the old buffer and unload it
	old_block->buffer.reset();
	old_block->state = BlockState::BLOCK_UNLOADED;
	old_block->memory_usage = 0;
	old_handle.Destroy();
	old_block.reset();

	// persist the new block to disk
	Write(*new_block->buffer, block_id);

	buffer_manager.GetBufferPool().AddToEvictionQueue(new_block);

	return new_block;
}

void BlockManager::UnregisterBlock(block_id_t block_id, bool can_destroy) {
	if (block_id >= MAXIMUM_BLOCK) {
		// in-memory buffer: buffer could have been offloaded to disk: remove the file
		buffer_manager.DeleteTemporaryFile(block_id);
	} else {
		lock_guard<mutex> lock(blocks_lock);
		// on-disk block: erase from list of blocks in manager
		blocks.erase(block_id);
	}
}

MetadataManager &BlockManager::GetMetadataManager() {
	return *metadata_manager;
}

void BlockManager::Truncate() {
}

} // namespace duckdb




namespace duckdb {

BufferHandle::BufferHandle() : handle(nullptr), node(nullptr) {
}

BufferHandle::BufferHandle(shared_ptr<BlockHandle> handle_p, FileBuffer *node_p)
    : handle(std::move(handle_p)), node(node_p) {
}

BufferHandle::BufferHandle(BufferHandle &&other) noexcept {
	std::swap(node, other.node);
	std::swap(handle, other.handle);
}

BufferHandle &BufferHandle::operator=(BufferHandle &&other) noexcept {
	std::swap(node, other.node);
	std::swap(handle, other.handle);
	return *this;
}

BufferHandle::~BufferHandle() {
	Destroy();
}

bool BufferHandle::IsValid() const {
	return node != nullptr;
}

void BufferHandle::Destroy() {
	if (!handle || !IsValid()) {
		return;
	}
	handle->block_manager.buffer_manager.Unpin(handle);
	handle.reset();
	node = nullptr;
}

FileBuffer &BufferHandle::GetFileBuffer() {
	D_ASSERT(node);
	return *node;
}

} // namespace duckdb




namespace duckdb {

typedef duckdb_moodycamel::ConcurrentQueue<BufferEvictionNode> eviction_queue_t;

struct EvictionQueue {
	eviction_queue_t q;
};

bool BufferEvictionNode::CanUnload(BlockHandle &handle_p) {
	if (timestamp != handle_p.eviction_timestamp) {
		// handle was used in between
		return false;
	}
	return handle_p.CanUnload();
}

shared_ptr<BlockHandle> BufferEvictionNode::TryGetBlockHandle() {
	auto handle_p = handle.lock();
	if (!handle_p) {
		// BlockHandle has been destroyed
		return nullptr;
	}
	if (!CanUnload(*handle_p)) {
		// handle was used in between
		return nullptr;
	}
	// this is the latest node in the queue with this handle
	return handle_p;
}

BufferPool::BufferPool(idx_t maximum_memory)
    : current_memory(0), maximum_memory(maximum_memory), queue(make_uniq<EvictionQueue>()), queue_insertions(0) {
}
BufferPool::~BufferPool() {
}

void BufferPool::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	constexpr int INSERT_INTERVAL = 1024;

	D_ASSERT(handle->readers == 0);
	handle->eviction_timestamp++;
	// After each 1024 insertions, run through the queue and purge.
	if ((++queue_insertions % INSERT_INTERVAL) == 0) {
		PurgeQueue();
	}
	queue->q.enqueue(BufferEvictionNode(weak_ptr<BlockHandle>(handle), handle->eviction_timestamp));
}

void BufferPool::IncreaseUsedMemory(idx_t size) {
	current_memory += size;
}

idx_t BufferPool::GetUsedMemory() {
	return current_memory;
}
idx_t BufferPool::GetMaxMemory() {
	return maximum_memory;
}

BufferPool::EvictionResult BufferPool::EvictBlocks(idx_t extra_memory, idx_t memory_limit,
                                                   unique_ptr<FileBuffer> *buffer) {
	BufferEvictionNode node;
	TempBufferPoolReservation r(*this, extra_memory);
	while (current_memory > memory_limit) {
		// get a block to unpin from the queue
		if (!queue->q.try_dequeue(node)) {
			// Failed to reserve. Adjust size of temp reservation to 0.
			r.Resize(0);
			return {false, std::move(r)};
		}
		// get a reference to the underlying block pointer
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		}
		// we might be able to free this block: grab the mutex and check if we can free it
		lock_guard<mutex> lock(handle->lock);
		if (!node.CanUnload(*handle)) {
			// something changed in the mean-time, bail out
			continue;
		}
		// hooray, we can unload the block
		if (buffer && handle->buffer->AllocSize() == extra_memory) {
			// we can actually re-use the memory directly!
			*buffer = handle->UnloadAndTakeBlock();
			return {true, std::move(r)};
		} else {
			// release the memory and mark the block as unloaded
			handle->Unload();
		}
	}
	return {true, std::move(r)};
}

void BufferPool::PurgeQueue() {
	BufferEvictionNode node;
	while (true) {
		if (!queue->q.try_dequeue(node)) {
			break;
		}
		auto handle = node.TryGetBlockHandle();
		if (!handle) {
			continue;
		} else {
			queue->q.enqueue(std::move(node));
			break;
		}
	}
}

void BufferPool::SetLimit(idx_t limit, const char *exception_postscript) {
	lock_guard<mutex> l_lock(limit_lock);
	// try to evict until the limit is reached
	if (!EvictBlocks(0, limit).success) {
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
	idx_t old_limit = maximum_memory;
	// set the global maximum memory to the new limit if successful
	maximum_memory = limit;
	// evict again
	if (!EvictBlocks(0, limit).success) {
		// failed: go back to old limit
		maximum_memory = old_limit;
		throw OutOfMemoryException(
		    "Failed to change memory limit to %lld: could not free up enough memory for the new limit%s", limit,
		    exception_postscript);
	}
}

} // namespace duckdb



namespace duckdb {

BufferPoolReservation::BufferPoolReservation(BufferPool &pool) : pool(pool) {
}

BufferPoolReservation::BufferPoolReservation(BufferPoolReservation &&src) noexcept : pool(src.pool) {
	size = src.size;
	src.size = 0;
}

BufferPoolReservation &BufferPoolReservation::operator=(BufferPoolReservation &&src) noexcept {
	size = src.size;
	src.size = 0;
	return *this;
}

BufferPoolReservation::~BufferPoolReservation() {
	D_ASSERT(size == 0);
}

void BufferPoolReservation::Resize(idx_t new_size) {
	int64_t delta = (int64_t)new_size - size;
	pool.IncreaseUsedMemory(delta);
	size = new_size;
}

void BufferPoolReservation::Merge(BufferPoolReservation &&src) {
	size += src.size;
	src.size = 0;
}

} // namespace duckdb






namespace duckdb {

unique_ptr<BufferManager> BufferManager::CreateStandardBufferManager(DatabaseInstance &db, DBConfig &config) {
	return make_uniq<StandardBufferManager>(db, config.options.temporary_directory);
}

shared_ptr<BlockHandle> BufferManager::RegisterSmallMemory(idx_t block_size) {
	throw NotImplementedException("This type of BufferManager can not create 'small-memory' blocks");
}

Allocator &BufferManager::GetBufferAllocator() {
	throw NotImplementedException("This type of BufferManager does not have an Allocator");
}

void BufferManager::ReserveMemory(idx_t size) {
	throw NotImplementedException("This type of BufferManager can not reserve memory");
}
void BufferManager::FreeReservedMemory(idx_t size) {
	throw NotImplementedException("This type of BufferManager can not free reserved memory");
}

void BufferManager::SetLimit(idx_t limit) {
	throw NotImplementedException("This type of BufferManager can not set a limit");
}

vector<TemporaryFileInformation> BufferManager::GetTemporaryFiles() {
	throw InternalException("This type of BufferManager does not allow temporary files");
}

const string &BufferManager::GetTemporaryDirectory() {
	throw InternalException("This type of BufferManager does not allow a temporary directory");
}

BufferPool &BufferManager::GetBufferPool() {
	throw InternalException("This type of BufferManager does not have a buffer pool");
}

void BufferManager::SetTemporaryDirectory(const string &new_dir) {
	throw NotImplementedException("This type of BufferManager can not set a temporary directory");
}

DatabaseInstance &BufferManager::GetDatabase() {
	throw NotImplementedException("This type of BufferManager is not linked to a DatabaseInstance");
}

bool BufferManager::HasTemporaryDirectory() const {
	return false;
}

unique_ptr<FileBuffer> BufferManager::ConstructManagedBuffer(idx_t size, unique_ptr<FileBuffer> &&source,
                                                             FileBufferType type) {
	throw NotImplementedException("This type of BufferManager can not construct managed buffers");
}

// Protected methods

void BufferManager::AddToEvictionQueue(shared_ptr<BlockHandle> &handle) {
	throw NotImplementedException("This type of BufferManager does not support 'AddToEvictionQueue");
}

void BufferManager::WriteTemporaryBuffer(block_id_t block_id, FileBuffer &buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'WriteTemporaryBuffer");
}

unique_ptr<FileBuffer> BufferManager::ReadTemporaryBuffer(block_id_t id, unique_ptr<FileBuffer> buffer) {
	throw NotImplementedException("This type of BufferManager does not support 'ReadTemporaryBuffer");
}

void BufferManager::DeleteTemporaryFile(block_id_t id) {
	throw NotImplementedException("This type of BufferManager does not support 'DeleteTemporaryFile");
}

} // namespace duckdb





namespace duckdb {

CompressionType RowGroupWriter::GetColumnCompressionType(idx_t i) {
	return table.GetColumn(LogicalIndex(i)).CompressionType();
}

void RowGroupWriter::RegisterPartialBlock(PartialBlockAllocation &&allocation) {
	partial_block_manager.RegisterPartialBlock(std::move(allocation));
}

PartialBlockAllocation RowGroupWriter::GetBlockAllocation(uint32_t segment_size) {
	return partial_block_manager.GetBlockAllocation(segment_size);
}

void SingleFileRowGroupWriter::WriteColumnDataPointers(ColumnCheckpointState &column_checkpoint_state,
                                                       Serializer &serializer) {
	const auto &data_pointers = column_checkpoint_state.data_pointers;
	serializer.WriteProperty(100, "data_pointers", data_pointers);
}

MetadataWriter &SingleFileRowGroupWriter::GetPayloadWriter() {
	return table_data_writer;
}

} // namespace duckdb










namespace duckdb {

TableDataReader::TableDataReader(MetadataReader &reader, BoundCreateTableInfo &info) : reader(reader), info(info) {
	info.data = make_uniq<PersistentTableData>(info.Base().columns.LogicalColumnCount());
}

void TableDataReader::ReadTableData() {
	auto &columns = info.Base().columns;
	D_ASSERT(!columns.empty());

	// We stored the table statistics as a unit in FinalizeTable.
	BinaryDeserializer stats_deserializer(reader);
	stats_deserializer.Begin();
	info.data->table_stats.Deserialize(stats_deserializer, columns);
	stats_deserializer.End();

	// Deserialize the row group pointers (lazily, just set the count and the pointer to them for now)
	info.data->row_group_count = reader.Read<uint64_t>();
	info.data->block_pointer = reader.GetMetaBlockPointer();
}

} // namespace duckdb








namespace duckdb {

TableDataWriter::TableDataWriter(TableCatalogEntry &table_p) : table(table_p.Cast<DuckTableEntry>()) {
	D_ASSERT(table_p.IsDuckTable());
}

TableDataWriter::~TableDataWriter() {
}

void TableDataWriter::WriteTableData(Serializer &metadata_serializer) {
	// start scanning the table and append the data to the uncompressed segments
	table.GetStorage().Checkpoint(*this, metadata_serializer);
}

CompressionType TableDataWriter::GetColumnCompressionType(idx_t i) {
	return table.GetColumn(LogicalIndex(i)).CompressionType();
}

void TableDataWriter::AddRowGroup(RowGroupPointer &&row_group_pointer, unique_ptr<RowGroupWriter> &&writer) {
	row_group_pointers.push_back(std::move(row_group_pointer));
	writer.reset();
}

SingleFileTableDataWriter::SingleFileTableDataWriter(SingleFileCheckpointWriter &checkpoint_manager,
                                                     TableCatalogEntry &table, MetadataWriter &table_data_writer)
    : TableDataWriter(table), checkpoint_manager(checkpoint_manager), table_data_writer(table_data_writer) {
}

unique_ptr<RowGroupWriter> SingleFileTableDataWriter::GetRowGroupWriter(RowGroup &row_group) {
	return make_uniq<SingleFileRowGroupWriter>(table, checkpoint_manager.partial_block_manager, table_data_writer);
}

void SingleFileTableDataWriter::FinalizeTable(TableStatistics &&global_stats, DataTableInfo *info,
                                              Serializer &metadata_serializer) {
	// store the current position in the metadata writer
	// this is where the row groups for this table start
	auto pointer = table_data_writer.GetMetaBlockPointer();

	// Serialize statistics as a single unit
	BinarySerializer stats_serializer(table_data_writer);
	stats_serializer.Begin();
	global_stats.Serialize(stats_serializer);
	stats_serializer.End();

	// now start writing the row group pointers to disk
	table_data_writer.Write<uint64_t>(row_group_pointers.size());
	idx_t total_rows = 0;
	for (auto &row_group_pointer : row_group_pointers) {
		auto row_group_count = row_group_pointer.row_start + row_group_pointer.tuple_count;
		if (row_group_count > total_rows) {
			total_rows = row_group_count;
		}

		// Each RowGroup is its own unit
		BinarySerializer row_group_serializer(table_data_writer);
		row_group_serializer.Begin();
		RowGroup::Serialize(row_group_pointer, row_group_serializer);
		row_group_serializer.End();
	}

	auto index_pointers = info->indexes.SerializeIndexes(table_data_writer);

	// Now begin the metadata as a unit
	// Pointer to the table itself goes to the metadata stream.
	metadata_serializer.WriteProperty(101, "table_pointer", pointer);
	metadata_serializer.WriteProperty(102, "total_rows", total_rows);
	metadata_serializer.WriteProperty(103, "index_pointers", index_pointers);
}

} // namespace duckdb




namespace duckdb {

WriteOverflowStringsToDisk::WriteOverflowStringsToDisk(BlockManager &block_manager)
    : block_manager(block_manager), block_id(INVALID_BLOCK), offset(0) {
}

WriteOverflowStringsToDisk::~WriteOverflowStringsToDisk() {
	// verify that the overflow writer has been flushed
	D_ASSERT(Exception::UncaughtException() || offset == 0);
}

shared_ptr<BlockHandle> UncompressedStringSegmentState::GetHandle(BlockManager &manager, block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	auto entry = handles.find(block_id);
	if (entry != handles.end()) {
		return entry->second;
	}
	auto result = manager.RegisterBlock(block_id);
	handles.insert(make_pair(block_id, result));
	return result;
}

void UncompressedStringSegmentState::RegisterBlock(BlockManager &manager, block_id_t block_id) {
	lock_guard<mutex> lock(block_lock);
	auto entry = handles.find(block_id);
	if (entry != handles.end()) {
		throw InternalException("UncompressedStringSegmentState::RegisterBlock - block id %llu already exists",
		                        block_id);
	}
	auto result = manager.RegisterBlock(block_id);
	handles.insert(make_pair(block_id, std::move(result)));
	on_disk_blocks.push_back(block_id);
}

void WriteOverflowStringsToDisk::WriteString(UncompressedStringSegmentState &state, string_t string,
                                             block_id_t &result_block, int32_t &result_offset) {
	auto &buffer_manager = block_manager.buffer_manager;
	if (!handle.IsValid()) {
		handle = buffer_manager.Allocate(Storage::BLOCK_SIZE);
	}
	// first write the length of the string
	if (block_id == INVALID_BLOCK || offset + 2 * sizeof(uint32_t) >= STRING_SPACE) {
		AllocateNewBlock(state, block_manager.GetFreeBlockId());
	}
	result_block = block_id;
	result_offset = offset;

	// write the length field
	auto data_ptr = handle.Ptr();
	auto string_length = string.GetSize();
	Store<uint32_t>(string_length, data_ptr + offset);
	offset += sizeof(uint32_t);

	// now write the remainder of the string
	auto strptr = string.GetData();
	uint32_t remaining = string_length;
	while (remaining > 0) {
		uint32_t to_write = MinValue<uint32_t>(remaining, STRING_SPACE - offset);
		if (to_write > 0) {
			memcpy(data_ptr + offset, strptr, to_write);

			remaining -= to_write;
			offset += to_write;
			strptr += to_write;
		}
		if (remaining > 0) {
			D_ASSERT(offset == WriteOverflowStringsToDisk::STRING_SPACE);
			// there is still remaining stuff to write
			// now write the current block to disk and allocate a new block
			AllocateNewBlock(state, block_manager.GetFreeBlockId());
		}
	}
}

void WriteOverflowStringsToDisk::Flush() {
	if (block_id != INVALID_BLOCK && offset > 0) {
		// zero-initialize the empty part of the overflow string buffer (if any)
		if (offset < STRING_SPACE) {
			memset(handle.Ptr() + offset, 0, STRING_SPACE - offset);
		}
		// write to disk
		block_manager.Write(handle.GetFileBuffer(), block_id);
	}
	block_id = INVALID_BLOCK;
	offset = 0;
}

void WriteOverflowStringsToDisk::AllocateNewBlock(UncompressedStringSegmentState &state, block_id_t new_block_id) {
	if (block_id != INVALID_BLOCK) {
		// there is an old block, write it first
		// write the new block id at the end of the previous block
		Store<block_id_t>(new_block_id, handle.Ptr() + WriteOverflowStringsToDisk::STRING_SPACE);
		Flush();
	}
	offset = 0;
	block_id = new_block_id;
	state.RegisterBlock(block_manager, new_block_id);
}

} // namespace duckdb































namespace duckdb {

void ReorderTableEntries(catalog_entry_vector_t &tables);

SingleFileCheckpointWriter::SingleFileCheckpointWriter(AttachedDatabase &db, BlockManager &block_manager)
    : CheckpointWriter(db), partial_block_manager(block_manager, CheckpointType::FULL_CHECKPOINT) {
}

BlockManager &SingleFileCheckpointWriter::GetBlockManager() {
	auto &storage_manager = db.GetStorageManager().Cast<SingleFileStorageManager>();
	return *storage_manager.block_manager;
}

MetadataWriter &SingleFileCheckpointWriter::GetMetadataWriter() {
	return *metadata_writer;
}

MetadataManager &SingleFileCheckpointWriter::GetMetadataManager() {
	return GetBlockManager().GetMetadataManager();
}

unique_ptr<TableDataWriter> SingleFileCheckpointWriter::GetTableDataWriter(TableCatalogEntry &table) {
	return make_uniq<SingleFileTableDataWriter>(*this, table, *table_metadata_writer);
}

static catalog_entry_vector_t GetCatalogEntries(vector<reference<SchemaCatalogEntry>> &schemas) {
	catalog_entry_vector_t entries;
	for (auto &schema_p : schemas) {
		auto &schema = schema_p.get();
		entries.push_back(schema);
		schema.Scan(CatalogType::TYPE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			entries.push_back(entry);
		});

		schema.Scan(CatalogType::SEQUENCE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			entries.push_back(entry);
		});

		catalog_entry_vector_t tables;
		vector<reference<ViewCatalogEntry>> views;
		schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::TABLE_ENTRY) {
				tables.push_back(entry.Cast<TableCatalogEntry>());
			} else if (entry.type == CatalogType::VIEW_ENTRY) {
				views.push_back(entry.Cast<ViewCatalogEntry>());
			} else {
				throw NotImplementedException("Catalog type for entries");
			}
		});
		// Reorder tables because of foreign key constraint
		ReorderTableEntries(tables);
		for (auto &table : tables) {
			entries.push_back(table.get());
		}
		for (auto &view : views) {
			entries.push_back(view.get());
		}

		schema.Scan(CatalogType::SCALAR_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::MACRO_ENTRY) {
				entries.push_back(entry);
			}
		});

		schema.Scan(CatalogType::TABLE_FUNCTION_ENTRY, [&](CatalogEntry &entry) {
			if (entry.internal) {
				return;
			}
			if (entry.type == CatalogType::TABLE_MACRO_ENTRY) {
				entries.push_back(entry);
			}
		});

		schema.Scan(CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
			D_ASSERT(!entry.internal);
			entries.push_back(entry);
		});
	}
	return entries;
}

void SingleFileCheckpointWriter::CreateCheckpoint() {
	auto &config = DBConfig::Get(db);
	auto &storage_manager = db.GetStorageManager().Cast<SingleFileStorageManager>();
	if (storage_manager.InMemory()) {
		return;
	}
	// assert that the checkpoint manager hasn't been used before
	D_ASSERT(!metadata_writer);

	auto &block_manager = GetBlockManager();
	auto &metadata_manager = GetMetadataManager();

	//! Set up the writers for the checkpoints
	metadata_writer = make_uniq<MetadataWriter>(metadata_manager);
	table_metadata_writer = make_uniq<MetadataWriter>(metadata_manager);

	// get the id of the first meta block
	auto meta_block = metadata_writer->GetMetaBlockPointer();

	vector<reference<SchemaCatalogEntry>> schemas;
	// we scan the set of committed schemas
	auto &catalog = Catalog::GetCatalog(db).Cast<DuckCatalog>();
	catalog.ScanSchemas([&](SchemaCatalogEntry &entry) { schemas.push_back(entry); });
	// write the actual data into the database

	// Create a serializer to write the checkpoint data
	// The serialized format is roughly:
	/*
	    {
	        schemas: [
	            {
	                schema: <schema_info>,
	                custom_types: [ { type: <type_info> }, ... ],
	                sequences: [ { sequence: <sequence_info> }, ... ],
	                tables: [ { table: <table_info> }, ... ],
	                views: [ { view: <view_info> }, ... ],
	                macros: [ { macro: <macro_info> }, ... ],
	                table_macros: [ { table_macro: <table_macro_info> }, ... ],
	                indexes: [ { index: <index_info>, root_offset <block_ptr> }, ... ]
	            }
	        ]
	    }
	 */
	auto catalog_entries = GetCatalogEntries(schemas);
	BinarySerializer serializer(*metadata_writer);
	serializer.Begin();
	serializer.WriteList(100, "catalog_entries", catalog_entries.size(), [&](Serializer::List &list, idx_t i) {
		auto &entry = catalog_entries[i];
		list.WriteObject([&](Serializer &obj) { WriteEntry(entry.get(), obj); });
	});
	serializer.End();

	partial_block_manager.FlushPartialBlocks();
	metadata_writer->Flush();
	table_metadata_writer->Flush();

	// write a checkpoint flag to the WAL
	// this protects against the rare event that the database crashes AFTER writing the file, but BEFORE truncating the
	// WAL we write an entry CHECKPOINT "meta_block_id" into the WAL upon loading, if we see there is an entry
	// CHECKPOINT "meta_block_id", and the id MATCHES the head idin the file we know that the database was successfully
	// checkpointed, so we know that we should avoid replaying the WAL to avoid duplicating data
	auto wal = storage_manager.GetWriteAheadLog();
	wal->WriteCheckpoint(meta_block);
	wal->Flush();

	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_HEADER) {
		throw FatalException("Checkpoint aborted before header write because of PRAGMA checkpoint_abort flag");
	}

	// finally write the updated header
	DatabaseHeader header;
	header.meta_block = meta_block.block_pointer;
	block_manager.WriteHeader(header);

	if (config.options.checkpoint_abort == CheckpointAbort::DEBUG_ABORT_BEFORE_TRUNCATE) {
		throw FatalException("Checkpoint aborted before truncate because of PRAGMA checkpoint_abort flag");
	}

	// truncate the file
	block_manager.Truncate();

	// truncate the WAL
	wal->Truncate(0);
}

void CheckpointReader::LoadCheckpoint(ClientContext &context, MetadataReader &reader) {
	BinaryDeserializer deserializer(reader);
	deserializer.Begin();
	deserializer.ReadList(100, "catalog_entries", [&](Deserializer::List &list, idx_t i) {
		return list.ReadObject([&](Deserializer &obj) { ReadEntry(context, obj); });
	});
	deserializer.End();
}

MetadataManager &SingleFileCheckpointReader::GetMetadataManager() {
	return storage.block_manager->GetMetadataManager();
}

void SingleFileCheckpointReader::LoadFromStorage() {
	auto &block_manager = *storage.block_manager;
	auto &metadata_manager = GetMetadataManager();
	MetaBlockPointer meta_block(block_manager.GetMetaBlock(), 0);
	if (!meta_block.IsValid()) {
		// storage is empty
		return;
	}

	Connection con(storage.GetDatabase());
	con.BeginTransaction();
	// create the MetadataReader to read from the storage
	MetadataReader reader(metadata_manager, meta_block);
	//	reader.SetContext(*con.context);
	LoadCheckpoint(*con.context, reader);
	con.Commit();
}

void CheckpointWriter::WriteEntry(CatalogEntry &entry, Serializer &serializer) {
	serializer.WriteProperty(99, "catalog_type", entry.type);

	switch (entry.type) {
	case CatalogType::SCHEMA_ENTRY: {
		auto &schema = entry.Cast<SchemaCatalogEntry>();
		WriteSchema(schema, serializer);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		auto &custom_type = entry.Cast<TypeCatalogEntry>();
		WriteType(custom_type, serializer);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		auto &seq = entry.Cast<SequenceCatalogEntry>();
		WriteSequence(seq, serializer);
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		auto &table = entry.Cast<TableCatalogEntry>();
		WriteTable(table, serializer);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		auto &view = entry.Cast<ViewCatalogEntry>();
		WriteView(view, serializer);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		auto &macro = entry.Cast<ScalarMacroCatalogEntry>();
		WriteMacro(macro, serializer);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		auto &macro = entry.Cast<TableMacroCatalogEntry>();
		WriteTableMacro(macro, serializer);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		auto &index = entry.Cast<IndexCatalogEntry>();
		WriteIndex(index, serializer);
		break;
	}
	default:
		throw InternalException("Unrecognized catalog type in CheckpointWriter::WriteEntry");
	}
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSchema(SchemaCatalogEntry &schema, Serializer &serializer) {
	// write the schema data
	serializer.WriteProperty(100, "schema", &schema);
}

void CheckpointReader::ReadEntry(ClientContext &context, Deserializer &deserializer) {
	auto type = deserializer.ReadProperty<CatalogType>(99, "type");

	switch (type) {
	case CatalogType::SCHEMA_ENTRY: {
		ReadSchema(context, deserializer);
		break;
	}
	case CatalogType::TYPE_ENTRY: {
		ReadType(context, deserializer);
		break;
	}
	case CatalogType::SEQUENCE_ENTRY: {
		ReadSequence(context, deserializer);
		break;
	}
	case CatalogType::TABLE_ENTRY: {
		ReadTable(context, deserializer);
		break;
	}
	case CatalogType::VIEW_ENTRY: {
		ReadView(context, deserializer);
		break;
	}
	case CatalogType::MACRO_ENTRY: {
		ReadMacro(context, deserializer);
		break;
	}
	case CatalogType::TABLE_MACRO_ENTRY: {
		ReadTableMacro(context, deserializer);
		break;
	}
	case CatalogType::INDEX_ENTRY: {
		ReadIndex(context, deserializer);
		break;
	}
	default:
		throw InternalException("Unrecognized catalog type in CheckpointWriter::WriteEntry");
	}
}

void CheckpointReader::ReadSchema(ClientContext &context, Deserializer &deserializer) {
	// Read the schema and create it in the catalog
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "schema");
	auto &schema_info = info->Cast<CreateSchemaInfo>();

	// we set create conflict to IGNORE_ON_CONFLICT, so that we can ignore a failure when recreating the main schema
	schema_info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	catalog.CreateSchema(context, schema_info);
}

//===--------------------------------------------------------------------===//
// Views
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteView(ViewCatalogEntry &view, Serializer &serializer) {
	serializer.WriteProperty(100, "view", &view);
}

void CheckpointReader::ReadView(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "view");
	auto &view_info = info->Cast<CreateViewInfo>();
	catalog.CreateView(context, view_info);
}

//===--------------------------------------------------------------------===//
// Sequences
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteSequence(SequenceCatalogEntry &seq, Serializer &serializer) {
	serializer.WriteProperty(100, "sequence", &seq);
}

void CheckpointReader::ReadSequence(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "sequence");
	auto &sequence_info = info->Cast<CreateSequenceInfo>();
	catalog.CreateSequence(context, sequence_info);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteIndex(IndexCatalogEntry &index_catalog, Serializer &serializer) {
	// The index data is written as part of WriteTableData.
	// Here, we need only serialize the pointer to that data.
	auto root_block_pointer = index_catalog.index->GetRootBlockPointer();
	serializer.WriteProperty(100, "index", &index_catalog);
	serializer.WriteProperty(101, "root_block_pointer", root_block_pointer);
}

void CheckpointReader::ReadIndex(ClientContext &context, Deserializer &deserializer) {

	// deserialize the index create info
	auto create_info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "index");
	auto &info = create_info->Cast<CreateIndexInfo>();

	// create the index in the catalog
	auto &schema = catalog.GetSchema(context, create_info->schema);
	auto &table =
	    catalog.GetEntry(context, CatalogType::TABLE_ENTRY, create_info->schema, info.table).Cast<DuckTableEntry>();

	auto &index = schema.CreateIndex(context, info, table)->Cast<DuckIndexEntry>();

	index.info = table.GetStorage().info;
	// insert the parsed expressions into the stored index so that we correctly (de)serialize it during consecutive
	// checkpoints
	for (auto &parsed_expr : info.parsed_expressions) {
		index.parsed_expressions.push_back(parsed_expr->Copy());
	}

	// we deserialize the index lazily, i.e., we do not need to load any node information
	// except the root block pointer
	auto root_block_pointer = deserializer.ReadProperty<BlockPointer>(101, "root_block_pointer");

	// obtain the parsed expressions of the ART from the index metadata
	vector<unique_ptr<ParsedExpression>> parsed_expressions;
	for (auto &parsed_expr : info.parsed_expressions) {
		parsed_expressions.push_back(parsed_expr->Copy());
	}
	D_ASSERT(!parsed_expressions.empty());

	// add the table to the bind context to bind the parsed expressions
	auto binder = Binder::CreateBinder(context);
	vector<LogicalType> column_types;
	vector<string> column_names;
	for (auto &col : table.GetColumns().Logical()) {
		column_types.push_back(col.Type());
		column_names.push_back(col.Name());
	}

	// create a binder to bind the parsed expressions
	vector<column_t> column_ids;
	binder->bind_context.AddBaseTable(0, info.table, column_names, column_types, column_ids, &table);
	IndexBinder idx_binder(*binder, context);

	// bind the parsed expressions to create unbound expressions
	vector<unique_ptr<Expression>> unbound_expressions;
	unbound_expressions.reserve(parsed_expressions.size());
	for (auto &expr : parsed_expressions) {
		unbound_expressions.push_back(idx_binder.Bind(expr));
	}

	// create the index and add it to the storage
	switch (info.index_type) {
	case IndexType::ART: {
		auto &storage = table.GetStorage();
		auto art = make_uniq<ART>(info.column_ids, TableIOManager::Get(storage), std::move(unbound_expressions),
		                          info.constraint_type, storage.db, nullptr, root_block_pointer);

		index.index = art.get();
		storage.info->indexes.AddIndex(std::move(art));
	} break;
	default:
		throw InternalException("Unknown index type for ReadIndex");
	}
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteType(TypeCatalogEntry &type, Serializer &serializer) {
	serializer.WriteProperty(100, "type", &type);
}

void CheckpointReader::ReadType(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "type");
	auto &type_info = info->Cast<CreateTypeInfo>();
	catalog.CreateType(context, type_info);
}

//===--------------------------------------------------------------------===//
// Macro's
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteMacro(ScalarMacroCatalogEntry &macro, Serializer &serializer) {
	serializer.WriteProperty(100, "macro", &macro);
}

void CheckpointReader::ReadMacro(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "macro");
	auto &macro_info = info->Cast<CreateMacroInfo>();
	catalog.CreateFunction(context, macro_info);
}

void CheckpointWriter::WriteTableMacro(TableMacroCatalogEntry &macro, Serializer &serializer) {
	serializer.WriteProperty(100, "table_macro", &macro);
}

void CheckpointReader::ReadTableMacro(ClientContext &context, Deserializer &deserializer) {
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "table_macro");
	auto &macro_info = info->Cast<CreateMacroInfo>();
	catalog.CreateFunction(context, macro_info);
}

//===--------------------------------------------------------------------===//
// Table Metadata
//===--------------------------------------------------------------------===//
void CheckpointWriter::WriteTable(TableCatalogEntry &table, Serializer &serializer) {
	// Write the table meta data
	serializer.WriteProperty(100, "table", &table);

	// Write the table data
	if (auto writer = GetTableDataWriter(table)) {
		writer->WriteTableData(serializer);
	}
}

void CheckpointReader::ReadTable(ClientContext &context, Deserializer &deserializer) {
	// deserialize the table meta data
	auto info = deserializer.ReadProperty<unique_ptr<CreateInfo>>(100, "table");
	auto binder = Binder::CreateBinder(context);
	auto &schema = catalog.GetSchema(context, info->schema);
	auto bound_info = binder->BindCreateTableInfo(std::move(info), schema);

	// now read the actual table data and place it into the create table info
	ReadTableData(context, deserializer, *bound_info);

	// finally create the table in the catalog
	catalog.CreateTable(context, *bound_info);
}

void CheckpointReader::ReadTableData(ClientContext &context, Deserializer &deserializer,
                                     BoundCreateTableInfo &bound_info) {

	// This is written in "SingleFileTableDataWriter::FinalizeTable"
	auto table_pointer = deserializer.ReadProperty<MetaBlockPointer>(101, "table_pointer");
	auto total_rows = deserializer.ReadProperty<idx_t>(102, "total_rows");
	auto index_pointers = deserializer.ReadProperty<vector<BlockPointer>>(103, "index_pointers");

	// FIXME: icky downcast to get the underlying MetadataReader
	auto &binary_deserializer = dynamic_cast<BinaryDeserializer &>(deserializer);
	auto &reader = dynamic_cast<MetadataReader &>(binary_deserializer.GetStream());

	MetadataReader table_data_reader(reader.GetMetadataManager(), table_pointer);
	TableDataReader data_reader(table_data_reader, bound_info);
	data_reader.ReadTableData();

	bound_info.data->total_rows = total_rows;
	bound_info.indexes = index_pointers;
}

} // namespace duckdb

















#include <functional>

namespace duckdb {

static constexpr const idx_t BITPACKING_METADATA_GROUP_SIZE = STANDARD_VECTOR_SIZE > 512 ? STANDARD_VECTOR_SIZE : 2048;

BitpackingMode BitpackingModeFromString(const string &str) {
	auto mode = StringUtil::Lower(str);
	if (mode == "auto" || mode == "none") {
		return BitpackingMode::AUTO;
	} else if (mode == "constant") {
		return BitpackingMode::CONSTANT;
	} else if (mode == "constant_delta") {
		return BitpackingMode::CONSTANT_DELTA;
	} else if (mode == "delta_for") {
		return BitpackingMode::DELTA_FOR;
	} else if (mode == "for") {
		return BitpackingMode::FOR;
	} else {
		return BitpackingMode::INVALID;
	}
}

string BitpackingModeToString(const BitpackingMode &mode) {
	switch (mode) {
	case BitpackingMode::AUTO:
		return "auto";
	case BitpackingMode::CONSTANT:
		return "constant";
	case BitpackingMode::CONSTANT_DELTA:
		return "constant_delta";
	case BitpackingMode::DELTA_FOR:
		return "delta_for";
	case BitpackingMode::FOR:
		return "for";
	default:
		throw NotImplementedException("Unknown bitpacking mode: " + to_string((uint8_t)mode) + "\n");
	}
}

typedef struct {
	BitpackingMode mode;
	uint32_t offset;
} bitpacking_metadata_t;

typedef uint32_t bitpacking_metadata_encoded_t;

static bitpacking_metadata_encoded_t EncodeMeta(bitpacking_metadata_t metadata) {
	D_ASSERT(metadata.offset <= 16777215); // max uint24_t
	bitpacking_metadata_encoded_t encoded_value = metadata.offset;
	encoded_value |= (uint8_t)metadata.mode << 24;
	return encoded_value;
}
static bitpacking_metadata_t DecodeMeta(bitpacking_metadata_encoded_t *metadata_encoded) {
	bitpacking_metadata_t metadata;
	metadata.mode = Load<BitpackingMode>(data_ptr_cast(metadata_encoded) + 3);
	metadata.offset = *metadata_encoded & 0x00FFFFFF;
	return metadata;
}

struct EmptyBitpackingWriter {
	template <class T>
	static void WriteConstant(T constant, idx_t count, void *data_ptr, bool all_invalid) {
	}
	template <class T, class T_S = typename MakeSigned<T>::type>
	static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T *values, bool *validity,
	                               void *data_ptr) {
	}
	template <class T, class T_S = typename MakeSigned<T>::type>
	static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference,
	                          T_S delta_offset, T *original_values, idx_t count, void *data_ptr) {
	}
	template <class T>
	static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count,
	                     void *data_ptr) {
	}
};

template <class T, class T_S = typename MakeSigned<T>::type>
struct BitpackingState {
public:
	BitpackingState() : compression_buffer_idx(0), total_size(0), data_ptr(nullptr) {
		compression_buffer_internal[0] = T(0);
		compression_buffer = &compression_buffer_internal[1];
		Reset();
	}

	// Extra val for delta encoding
	T compression_buffer_internal[BITPACKING_METADATA_GROUP_SIZE + 1];
	T *compression_buffer;
	T_S delta_buffer[BITPACKING_METADATA_GROUP_SIZE];
	bool compression_buffer_validity[BITPACKING_METADATA_GROUP_SIZE];
	idx_t compression_buffer_idx;
	idx_t total_size;

	// Used to pass CompressionState ptr through the Bitpacking writer
	void *data_ptr;

	// Stats on current compression buffer
	T minimum;
	T maximum;
	T min_max_diff;
	T_S minimum_delta;
	T_S maximum_delta;
	T_S min_max_delta_diff;
	T_S delta_offset;
	bool all_valid;
	bool all_invalid;

	bool can_do_delta;
	bool can_do_for;

	// Used to force a specific mode, useful in testing
	BitpackingMode mode = BitpackingMode::AUTO;

public:
	void Reset() {
		minimum = NumericLimits<T>::Maximum();
		minimum_delta = NumericLimits<T_S>::Maximum();
		maximum = NumericLimits<T>::Minimum();
		maximum_delta = NumericLimits<T_S>::Minimum();
		delta_offset = 0;
		all_valid = true;
		all_invalid = true;
		can_do_delta = false;
		can_do_for = false;
		compression_buffer_idx = 0;
		min_max_diff = 0;
		min_max_delta_diff = 0;
	}

	void CalculateFORStats() {
		can_do_for = TrySubtractOperator::Operation(maximum, minimum, min_max_diff);
	}

	void CalculateDeltaStats() {
		// TODO: currently we dont support delta compression of values above NumericLimits<T_S>::Maximum(),
		// 		 we could support this with some clever substract trickery?
		if (maximum > static_cast<T>(NumericLimits<T_S>::Maximum())) {
			return;
		}

		// Don't delta encoding 1 value makes no sense
		if (compression_buffer_idx < 2) {
			return;
		}

		// TODO: handle NULLS here?
		// Currently we cannot handle nulls because we would need an additional step of patching for this.
		// we could for example copy the last value on a null insert. This would help a bit, but not be optimal for
		// large deltas since theres suddenly a zero then. Ideally we would insert a value that leads to a delta within
		// the current domain of deltas however we dont know that domain here yet
		if (!all_valid) {
			return;
		}

		// Note: since we dont allow any values over NumericLimits<T_S>::Maximum(), all subtractions for unsigned types
		// are guaranteed not to overflow
		bool can_do_all = true;
		if (NumericLimits<T>::IsSigned()) {
			T_S bogus;
			can_do_all = TrySubtractOperator::Operation(static_cast<T_S>(minimum), static_cast<T_S>(maximum), bogus) &&
			             TrySubtractOperator::Operation(static_cast<T_S>(maximum), static_cast<T_S>(minimum), bogus);
		}

		// Calculate delta's
		// compression_buffer pointer points one element ahead of the internal buffer making the use of signed index
		// integer (-1) possible
		D_ASSERT(compression_buffer_idx <= NumericLimits<int64_t>::Maximum());
		if (can_do_all) {
			for (int64_t i = 0; i < static_cast<int64_t>(compression_buffer_idx); i++) {
				delta_buffer[i] = static_cast<T_S>(compression_buffer[i]) - static_cast<T_S>(compression_buffer[i - 1]);
			}
		} else {
			for (int64_t i = 0; i < static_cast<int64_t>(compression_buffer_idx); i++) {
				auto success =
				    TrySubtractOperator::Operation(static_cast<T_S>(compression_buffer[i]),
				                                   static_cast<T_S>(compression_buffer[i - 1]), delta_buffer[i]);
				if (!success) {
					return;
				}
			}
		}

		can_do_delta = true;

		for (idx_t i = 1; i < compression_buffer_idx; i++) {
			maximum_delta = MaxValue<T_S>(maximum_delta, delta_buffer[i]);
			minimum_delta = MinValue<T_S>(minimum_delta, delta_buffer[i]);
		}

		// Since we can set the first value arbitrarily, we want to pick one from the current domain, note that
		// we will store the original first value - this offset as the  delta_offset to be able to decode this again.
		delta_buffer[0] = minimum_delta;

		can_do_delta = can_do_delta && TrySubtractOperator::Operation(maximum_delta, minimum_delta, min_max_delta_diff);
		can_do_delta = can_do_delta && TrySubtractOperator::Operation(static_cast<T_S>(compression_buffer[0]),
		                                                              minimum_delta, delta_offset);
	}

	template <class T_INNER>
	void SubtractFrameOfReference(T_INNER *buffer, T_INNER frame_of_reference) {
		static_assert(IsIntegral<T_INNER>::value, "Integral type required.");
		for (idx_t i = 0; i < compression_buffer_idx; i++) {
			buffer[i] -= static_cast<typename MakeUnsigned<T_INNER>::type>(frame_of_reference);
		}
	}

	template <class OP>
	bool Flush() {
		if (compression_buffer_idx == 0) {
			return true;
		}

		if ((all_invalid || maximum == minimum) && (mode == BitpackingMode::AUTO || mode == BitpackingMode::CONSTANT)) {
			OP::WriteConstant(maximum, compression_buffer_idx, data_ptr, all_invalid);
			total_size += sizeof(T) + sizeof(bitpacking_metadata_encoded_t);
			return true;
		}

		CalculateFORStats();
		CalculateDeltaStats();

		if (can_do_delta) {
			if (maximum_delta == minimum_delta && mode != BitpackingMode::FOR && mode != BitpackingMode::DELTA_FOR) {
				// FOR needs to be T (considering hugeint is bigger than idx_t)
				T frame_of_reference = compression_buffer[0];

				OP::WriteConstantDelta(maximum_delta, static_cast<T>(frame_of_reference), compression_buffer_idx,
				                       compression_buffer, compression_buffer_validity, data_ptr);
				total_size += sizeof(T) + sizeof(T) + sizeof(bitpacking_metadata_encoded_t);
				return true;
			}

			// Check if delta has benefit
			// bitwidth is calculated differently between signed and unsigned values, but considering we do not have
			// an unsigned version of hugeint, we need to explicitly specify (through boolean) that we wish to calculate
			// the unsigned minimum bit-width instead of relying on MakeUnsigned and IsSigned
			auto delta_required_bitwidth = BitpackingPrimitives::MinimumBitWidth<T, false>(min_max_delta_diff);
			auto regular_required_bitwidth = BitpackingPrimitives::MinimumBitWidth(min_max_diff);

			if (delta_required_bitwidth < regular_required_bitwidth && mode != BitpackingMode::FOR) {
				SubtractFrameOfReference(delta_buffer, minimum_delta);

				OP::WriteDeltaFor(reinterpret_cast<T *>(delta_buffer), compression_buffer_validity,
				                  delta_required_bitwidth, static_cast<T>(minimum_delta), delta_offset,
				                  compression_buffer, compression_buffer_idx, data_ptr);

				total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, delta_required_bitwidth);
				total_size += sizeof(T);                              // FOR value
				total_size += sizeof(T);                              // Delta offset value
				total_size += AlignValue(sizeof(bitpacking_width_t)); // FOR value

				return true;
			}
		}

		if (can_do_for) {
			auto width = BitpackingPrimitives::MinimumBitWidth<T, false>(min_max_diff);
			SubtractFrameOfReference(compression_buffer, minimum);
			OP::WriteFor(compression_buffer, compression_buffer_validity, width, minimum, compression_buffer_idx,
			             data_ptr);

			total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, width);
			total_size += sizeof(T); // FOR value
			total_size += AlignValue(sizeof(bitpacking_width_t));

			return true;
		}

		return false;
	}

	template <class OP = EmptyBitpackingWriter>
	bool Update(T value, bool is_valid) {
		compression_buffer_validity[compression_buffer_idx] = is_valid;
		all_valid = all_valid && is_valid;
		all_invalid = all_invalid && !is_valid;

		if (is_valid) {
			compression_buffer[compression_buffer_idx] = value;
			minimum = MinValue<T>(minimum, value);
			maximum = MaxValue<T>(maximum, value);
		}

		compression_buffer_idx++;

		if (compression_buffer_idx == BITPACKING_METADATA_GROUP_SIZE) {
			bool success = Flush<OP>();
			Reset();
			return success;
		}
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingAnalyzeState : public AnalyzeState {
	BitpackingState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto &config = DBConfig::GetConfig(col_data.GetDatabase());

	auto state = make_uniq<BitpackingAnalyzeState<T>>();
	state->state.mode = config.options.force_bitpacking_mode;

	return std::move(state);
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = static_cast<BitpackingAnalyzeState<T> &>(state);
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!analyze_state.state.template Update<EmptyBitpackingWriter>(data[idx], vdata.validity.RowIsValid(idx))) {
			return false;
		}
	}
	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = static_cast<BitpackingAnalyzeState<T> &>(state);
	auto flush_result = bitpacking_state.state.template Flush<EmptyBitpackingWriter>();
	if (!flush_result) {
		return DConstants::INVALID_INDEX;
	}
	return bitpacking_state.state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS, class T_S = typename MakeSigned<T>::type>
struct BitpackingCompressState : public CompressionState {
public:
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer)
	    : checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = reinterpret_cast<void *>(this);

		auto &config = DBConfig::GetConfig(checkpointer.GetDatabase());
		state.mode = config.options.force_bitpacking_mode;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths and frame-of-references (growing downwards).
	data_ptr_t metadata_ptr;

	BitpackingState<T> state;

public:
	struct BitpackingWriter {
		static void WriteConstant(T constant, idx_t count, void *data_ptr, bool all_invalid) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}

		static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T *values, bool *validity,
		                               void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, 2 * sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT_DELTA);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}
		static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference,
		                          T_S delta_offset, T *original_values, idx_t count, void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
			ReserveSpace(state, bp_size + 3 * sizeof(T));

			WriteMetaData(state, BitpackingMode::DELTA_FOR);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, static_cast<T>(width));
			WriteData(state->data_ptr, delta_offset);

			BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			state->data_ptr += bp_size;

			UpdateStats(state, count);
		}

		static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count,
		                     void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
			ReserveSpace(state, bp_size + 2 * sizeof(T));

			WriteMetaData(state, BitpackingMode::FOR);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, (T)width);

			BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			state->data_ptr += bp_size;

			UpdateStats(state, count);
		}

		template <class T_OUT>
		static void WriteData(data_ptr_t &ptr, T_OUT val) {
			*reinterpret_cast<T_OUT *>(ptr) = val;
			ptr += sizeof(T_OUT);
		}

		static void WriteMetaData(BitpackingCompressState<T, WRITE_STATISTICS> *state, BitpackingMode mode) {
			bitpacking_metadata_t metadata {mode, (uint32_t)(state->data_ptr - state->handle.Ptr())};
			state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
			Store<bitpacking_metadata_encoded_t>(EncodeMeta(metadata), state->metadata_ptr);
		}

		static void ReserveSpace(BitpackingCompressState<T, WRITE_STATISTICS> *state, idx_t data_bytes) {
			idx_t meta_bytes = sizeof(bitpacking_metadata_encoded_t);
			state->FlushAndCreateSegmentIfFull(data_bytes, meta_bytes);
			D_ASSERT(state->CanStore(data_bytes, meta_bytes));
		}

		static void UpdateStats(BitpackingCompressState<T, WRITE_STATISTICS> *state, idx_t count) {
			state->current_segment->count += count;

			if (WRITE_STATISTICS && !state->state.all_invalid) {
				NumericStats::Update<T>(state->current_segment->stats.statistics, state->state.minimum);
				NumericStats::Update<T>(state->current_segment->stats.statistics, state->state.maximum);
			}
		}
	};

	bool CanStore(idx_t data_bytes, idx_t meta_bytes) {
		auto required_data_bytes = AlignValue<idx_t>((data_ptr + data_bytes) - data_ptr);
		auto required_meta_bytes = Storage::BLOCK_SIZE - (metadata_ptr - data_ptr) + meta_bytes;

		return required_data_bytes + required_meta_bytes <=
		       Storage::BLOCK_SIZE - BitpackingPrimitives::BITPACKING_HEADER_SIZE;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = std::move(compressed_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle.Ptr() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		metadata_ptr = handle.Ptr() + Storage::BLOCK_SIZE;
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = UnifiedVectorFormat::GetData<T>(vdata);

		for (idx_t i = 0; i < count; i++) {
			idx_t idx = vdata.sel->get_index(i);
			state.template Update<BitpackingCompressState<T, WRITE_STATISTICS, T_S>::BitpackingWriter>(
			    data[idx], vdata.validity.RowIsValid(idx));
		}
	}

	void FlushAndCreateSegmentIfFull(idx_t required_data_bytes, idx_t required_meta_bytes) {
		if (!CanStore(required_data_bytes, required_meta_bytes)) {
			idx_t row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		auto base_ptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t metadata_offset = AlignValue(data_ptr - base_ptr);
		idx_t metadata_size = base_ptr + Storage::BLOCK_SIZE - metadata_ptr;
		idx_t total_segment_size = metadata_offset + metadata_size;

		// Asserting things are still sane here
		if (!CanStore(0, 0)) {
			throw InternalException("Error in bitpacking size calculation");
		}

		memmove(base_ptr + metadata_offset, metadata_ptr, metadata_size);

		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<idx_t>(metadata_offset + metadata_size, base_ptr);
		handle.Destroy();

		state.FlushSegment(std::move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressState<T, WRITE_STATISTICS, T_S>::BitpackingWriter>();
		FlushSegment();
		current_segment.reset();
	}
};

template <class T, bool WRITE_STATISTICS>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {
	return make_uniq<BitpackingCompressState<T, WRITE_STATISTICS>>(checkpointer);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = static_cast<BitpackingCompressState<T, WRITE_STATISTICS> &>(state_p);
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	state.Append(vdata, count);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = static_cast<BitpackingCompressState<T, WRITE_STATISTICS> &>(state_p);
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
static void ApplyFrameOfReference(T *dst, T frame_of_reference, idx_t size) {
	if (!frame_of_reference) {
		return;
	}
	for (idx_t i = 0; i < size; i++) {
		dst[i] += frame_of_reference;
	}
}

// Based on https://github.com/lemire/FastPFor (Apache License 2.0)
template <class T>
static T DeltaDecode(T *data, T previous_value, const size_t size) {
	D_ASSERT(size >= 1);

	data[0] += previous_value;

	const size_t UnrollQty = 4;
	const size_t sz0 = (size / UnrollQty) * UnrollQty; // equal to 0, if size < UnrollQty
	size_t i = 1;
	if (sz0 >= UnrollQty) {
		T a = data[0];
		for (; i < sz0 - UnrollQty; i += UnrollQty) {
			a = data[i] += a;
			a = data[i + 1] += a;
			a = data[i + 2] += a;
			a = data[i + 3] += a;
		}
	}
	for (; i != size; ++i) {
		data[i] += data[i - 1];
	}

	return data[size - 1];
}

template <class T, class T_S = typename MakeSigned<T>::type>
struct BitpackingScanState : public SegmentScanState {
public:
	explicit BitpackingScanState(ColumnSegment &segment) : current_segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();

		// load offset to bitpacking widths pointer
		auto bitpacking_metadata_offset = Load<idx_t>(dataptr + segment.GetBlockOffset());
		bitpacking_metadata_ptr =
		    dataptr + segment.GetBlockOffset() + bitpacking_metadata_offset - sizeof(bitpacking_metadata_encoded_t);

		// load the first group
		LoadNextGroup();
	}

	BufferHandle handle;
	ColumnSegment &current_segment;

	T decompression_buffer[BITPACKING_METADATA_GROUP_SIZE];

	bitpacking_metadata_t current_group;

	bitpacking_width_t current_width;
	T current_frame_of_reference;
	T current_constant;
	T current_delta_offset;

	idx_t current_group_offset = 0;
	data_ptr_t current_group_ptr;
	data_ptr_t bitpacking_metadata_ptr;

public:
	//! Loads the metadata for the current metadata group. This will set bitpacking_metadata_ptr to the next group.
	//! this will also load any metadata that is at the start of a compressed buffer (e.g. the width, for, or constant
	//! value) depending on the bitpacking mode for that group
	void LoadNextGroup() {
		D_ASSERT(bitpacking_metadata_ptr > handle.Ptr() &&
		         bitpacking_metadata_ptr < handle.Ptr() + Storage::BLOCK_SIZE);
		current_group_offset = 0;
		current_group = DecodeMeta(reinterpret_cast<bitpacking_metadata_encoded_t *>(bitpacking_metadata_ptr));

		bitpacking_metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
		current_group_ptr = GetPtr(current_group);

		// Read first value
		switch (current_group.mode) {
		case BitpackingMode::CONSTANT:
			current_constant = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::FOR:
		case BitpackingMode::CONSTANT_DELTA:
		case BitpackingMode::DELTA_FOR:
			current_frame_of_reference = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		default:
			throw InternalException("Invalid bitpacking mode");
		}

		// Read second value
		switch (current_group.mode) {
		case BitpackingMode::CONSTANT_DELTA:
			current_constant = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::FOR:
		case BitpackingMode::DELTA_FOR:
			current_width = (bitpacking_width_t)(*reinterpret_cast<T *>(current_group_ptr));
			current_group_ptr += MaxValue(sizeof(T), sizeof(bitpacking_width_t));
			break;
		case BitpackingMode::CONSTANT:
			break;
		default:
			throw InternalException("Invalid bitpacking mode");
		}

		// Read third value
		if (current_group.mode == BitpackingMode::DELTA_FOR) {
			current_delta_offset = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
		}
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		bool skip_sign_extend = true;

		idx_t skipped = 0;
		while (skipped < skip_count) {
			// Exhausted this metadata group, move pointers to next group and load metadata for next group.
			if (current_group_offset >= BITPACKING_METADATA_GROUP_SIZE) {
				LoadNextGroup();
			}

			idx_t offset_in_compression_group =
			    current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

			if (current_group.mode == BitpackingMode::CONSTANT) {
				idx_t remaining = skip_count - skipped;
				idx_t to_skip = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - current_group_offset);
				skipped += to_skip;
				current_group_offset += to_skip;
				continue;
			}
			if (current_group.mode == BitpackingMode::CONSTANT_DELTA) {
				idx_t remaining = skip_count - skipped;
				idx_t to_skip = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - current_group_offset);
				skipped += to_skip;
				current_group_offset += to_skip;
				continue;
			}
			D_ASSERT(current_group.mode == BitpackingMode::FOR || current_group.mode == BitpackingMode::DELTA_FOR);

			idx_t to_skip =
			    MinValue<idx_t>(skip_count - skipped,
			                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);
			// Calculate start of compression algorithm group
			if (current_group.mode == BitpackingMode::DELTA_FOR) {
				data_ptr_t current_position_ptr = current_group_ptr + current_group_offset * current_width / 8;
				data_ptr_t decompression_group_start_pointer =
				    current_position_ptr - offset_in_compression_group * current_width / 8;

				BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(decompression_buffer),
				                                     decompression_group_start_pointer, current_width,
				                                     skip_sign_extend);

				T *decompression_ptr = decompression_buffer + offset_in_compression_group;
				ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
				                           static_cast<T_S>(current_frame_of_reference), to_skip);
				DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr), static_cast<T_S>(current_delta_offset),
				                 to_skip);
				current_delta_offset = decompression_ptr[to_skip - 1];
			}

			skipped += to_skip;
			current_group_offset += to_skip;
		}
	}

	data_ptr_t GetPtr(bitpacking_metadata_t group) {
		return handle.Ptr() + current_segment.GetBlockOffset() + group.offset;
	}
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_uniq<BitpackingScanState<T>>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T, class T_S = typename MakeSigned<T>::type>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = static_cast<BitpackingScanState<T> &>(*state.scan_state);

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	idx_t scanned = 0;
	while (scanned < scan_count) {
		// Exhausted this metadata group, move pointers to next group and load metadata for next group.
		if (scan_state.current_group_offset >= BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}

		idx_t offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T *begin = result_data + result_offset + scanned;
			T *end = begin + remaining;
			std::fill(begin, end, scan_state.current_constant);
			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T *target_ptr = result_data + result_offset + scanned;

			for (idx_t i = 0; i < to_scan; i++) {
				target_ptr[i] = (static_cast<T>(scan_state.current_group_offset + i) * scan_state.current_constant) +
				                scan_state.current_frame_of_reference;
			}

			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);
		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		T *current_result_ptr = result_data + result_offset + scanned;

		if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
			// Decompress directly into result vector
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(current_result_ptr), decompression_group_start_pointer,
			                                     scan_state.current_width, skip_sign_extend);
		} else {
			// Decompress compression algorithm to buffer
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
			                                     decompression_group_start_pointer, scan_state.current_width,
			                                     skip_sign_extend);

			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
		}

		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
			                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = current_result_ptr[to_scan - 1];
		} else {
			ApplyFrameOfReference<T>(current_result_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		scanned += to_scan;
		scan_state.current_group_offset += to_scan;
	}
}

template <class T>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	BitpackingScanPartial<T>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                        idx_t result_idx) {
	BitpackingScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);
	T *result_data = FlatVector::GetData<T>(result);
	T *current_result_ptr = result_data + result_idx;

	idx_t offset_in_compression_group =
	    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	data_ptr_t decompression_group_start_pointer =
	    scan_state.current_group_ptr +
	    (scan_state.current_group_offset - offset_in_compression_group) * scan_state.current_width / 8;

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
		*current_result_ptr = scan_state.current_constant;
		return;
	}

	if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
#ifdef DEBUG
		// overflow check
		T result;
		bool multiply = TryMultiplyOperator::Operation(static_cast<T>(scan_state.current_group_offset),
		                                               scan_state.current_constant, result);
		bool add = TryAddOperator::Operation(result, scan_state.current_frame_of_reference, result);
		D_ASSERT(multiply && add);
#endif
		*current_result_ptr = (static_cast<T>(scan_state.current_group_offset) * scan_state.current_constant) +
		                      scan_state.current_frame_of_reference;
		return;
	}

	D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
	         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

	BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
	                                     decompression_group_start_pointer, scan_state.current_width, skip_sign_extend);

	*current_result_ptr = scan_state.decompression_buffer[offset_in_compression_group];
	*current_result_ptr += scan_state.current_frame_of_reference;

	if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
		*current_result_ptr += scan_state.current_delta_offset;
	}
}
template <class T>
void BitpackingSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = static_cast<BitpackingScanState<T> &>(*state.scan_state);
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS = true>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_BITPACKING, data_type, BitpackingInitAnalyze<T>,
	                           BitpackingAnalyze<T>, BitpackingFinalAnalyze<T>,
	                           BitpackingInitCompression<T, WRITE_STATISTICS>, BitpackingCompress<T, WRITE_STATISTICS>,
	                           BitpackingFinalizeCompress<T, WRITE_STATISTICS>, BitpackingInitScan<T>,
	                           BitpackingScan<T>, BitpackingScanPartial<T>, BitpackingFetchRow<T>, BitpackingSkip<T>);
}

CompressionFunction BitpackingFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetBitpackingFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetBitpackingFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
	case PhysicalType::UINT8:
		return GetBitpackingFunction<uint8_t>(type);
	case PhysicalType::UINT16:
		return GetBitpackingFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetBitpackingFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetBitpackingFunction<uint64_t>(type);
	case PhysicalType::INT128:
		return GetBitpackingFunction<hugeint_t>(type);
	case PhysicalType::LIST:
		return GetBitpackingFunction<uint64_t, false>(type);
	default:
		throw InternalException("Unsupported type for Bitpacking");
	}
}

bool BitpackingFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::LIST:
	case PhysicalType::INT128:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb


namespace duckdb {

//===--------------------------------------------------------------------===//
// Unpacking
//===--------------------------------------------------------------------===//

static void UnpackSingle(const uint32_t *__restrict &in, hugeint_t *__restrict out, uint16_t delta, uint16_t shr) {
	if (delta + shr < 32) {
		*out = ((static_cast<hugeint_t>(in[0])) >> shr) % (hugeint_t(1) << delta);
	}

	else if (delta + shr >= 32 && delta + shr < 64) {
		*out = static_cast<hugeint_t>(in[0]) >> shr;
		++in;

		if (delta + shr > 32) {
			const uint16_t NEXT_SHR = shr + delta - 32;
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (32 - shr);
		}
	}

	else if (delta + shr >= 64 && delta + shr < 96) {
		*out = static_cast<hugeint_t>(in[0]) >> shr;
		*out |= static_cast<hugeint_t>(in[1]) << (32 - shr);
		in += 2;

		if (delta + shr > 64) {
			const uint16_t NEXT_SHR = delta + shr - 64;
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (64 - shr);
		}
	}

	else if (delta + shr >= 96 && delta + shr < 128) {
		*out = static_cast<hugeint_t>(in[0]) >> shr;
		*out |= static_cast<hugeint_t>(in[1]) << (32 - shr);
		*out |= static_cast<hugeint_t>(in[2]) << (64 - shr);
		in += 3;

		if (delta + shr > 96) {
			const uint16_t NEXT_SHR = delta + shr - 96;
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (96 - shr);
		}
	}

	else if (delta + shr >= 128) {
		*out = static_cast<hugeint_t>(in[0]) >> shr;
		*out |= static_cast<hugeint_t>(in[1]) << (32 - shr);
		*out |= static_cast<hugeint_t>(in[2]) << (64 - shr);
		*out |= static_cast<hugeint_t>(in[3]) << (96 - shr);
		in += 4;

		if (delta + shr > 128) {
			const uint16_t NEXT_SHR = delta + shr - 128;
			*out |= static_cast<hugeint_t>((*in) % (1U << NEXT_SHR)) << (128 - shr);
		}
	}
}

static void UnpackLast(const uint32_t *__restrict &in, hugeint_t *__restrict out, uint16_t delta) {
	const uint8_t LAST_IDX = 31;
	const uint16_t SHIFT = (delta * 31) % 32;
	out[LAST_IDX] = in[0] >> SHIFT;
	if (delta > 32) {
		out[LAST_IDX] |= static_cast<hugeint_t>(in[1]) << (32 - SHIFT);
	}
	if (delta > 64) {
		out[LAST_IDX] |= static_cast<hugeint_t>(in[2]) << (64 - SHIFT);
	}
	if (delta > 96) {
		out[LAST_IDX] |= static_cast<hugeint_t>(in[3]) << (96 - SHIFT);
	}
}

// Unpacks for specific deltas
static void UnpackDelta0(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		out[i] = 0;
	}
}

static void UnpackDelta32(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t k = 0; k < 32; ++k) {
		out[k] = static_cast<hugeint_t>(in[k]);
	}
}

static void UnpackDelta64(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		const uint8_t OFFSET = i * 2;
		out[i] = in[OFFSET];
		out[i] |= static_cast<hugeint_t>(in[OFFSET + 1]) << 32;
	}
}

static void UnpackDelta96(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		const uint8_t OFFSET = i * 3;
		out[i] = in[OFFSET];
		out[i] |= static_cast<hugeint_t>(in[OFFSET + 1]) << 32;
		out[i] |= static_cast<hugeint_t>(in[OFFSET + 2]) << 64;
	}
}

static void UnpackDelta128(const uint32_t *__restrict in, hugeint_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		const uint8_t OFFSET = i * 4;
		out[i] = in[OFFSET];
		out[i] |= static_cast<hugeint_t>(in[OFFSET + 1]) << 32;
		out[i] |= static_cast<hugeint_t>(in[OFFSET + 2]) << 64;
		out[i] |= static_cast<hugeint_t>(in[OFFSET + 3]) << 96;
	}
}

//===--------------------------------------------------------------------===//
// Packing
//===--------------------------------------------------------------------===//

static void PackSingle(const hugeint_t in, uint32_t *__restrict &out, uint16_t delta, uint16_t shl, hugeint_t mask) {
	if (delta + shl < 32) {

		if (shl == 0) {
			out[0] = static_cast<uint32_t>(in & mask);
		} else {
			out[0] |= static_cast<uint32_t>((in & mask) << shl);
		}

	} else if (delta + shl >= 32 && delta + shl < 64) {

		if (shl == 0) {
			out[0] = static_cast<uint32_t>(in & mask);
		} else {
			out[0] |= static_cast<uint32_t>((in & mask) << shl);
		}
		++out;

		if (delta + shl > 32) {
			*out = static_cast<uint32_t>((in & mask) >> (32 - shl));
		}
	}

	else if (delta + shl >= 64 && delta + shl < 96) {

		if (shl == 0) {
			out[0] = static_cast<uint32_t>(in & mask);
		} else {
			out[0] |= static_cast<uint32_t>(in << shl);
		}

		out[1] = static_cast<uint32_t>((in & mask) >> (32 - shl));
		out += 2;

		if (delta + shl > 64) {
			*out = static_cast<uint32_t>((in & mask) >> (64 - shl));
		}
	}

	else if (delta + shl >= 96 && delta + shl < 128) {
		if (shl == 0) {
			out[0] = static_cast<uint32_t>(in & mask);
		} else {
			out[0] |= static_cast<uint32_t>(in << shl);
		}

		out[1] = static_cast<uint32_t>((in & mask) >> (32 - shl));
		out[2] = static_cast<uint32_t>((in & mask) >> (64 - shl));
		out += 3;

		if (delta + shl > 96) {
			*out = static_cast<uint32_t>((in & mask) >> (96 - shl));
		}
	}

	else if (delta + shl >= 128) {
		// shl == 0 won't ever happen here considering a delta of 128 calls PackDelta128
		out[0] |= static_cast<uint32_t>(in << shl);
		out[1] = static_cast<uint32_t>((in & mask) >> (32 - shl));
		out[2] = static_cast<uint32_t>((in & mask) >> (64 - shl));
		out[3] = static_cast<uint32_t>((in & mask) >> (96 - shl));
		out += 4;

		if (delta + shl > 128) {
			*out = static_cast<uint32_t>((in & mask) >> (128 - shl));
		}
	}
}

static void PackLast(const hugeint_t *__restrict in, uint32_t *__restrict out, uint16_t delta) {
	const uint8_t LAST_IDX = 31;
	const uint16_t SHIFT = (delta * 31) % 32;
	out[0] |= static_cast<uint32_t>(in[LAST_IDX] << SHIFT);
	if (delta > 32) {
		out[1] = static_cast<uint32_t>(in[LAST_IDX] >> (32 - SHIFT));
	}
	if (delta > 64) {
		out[2] = static_cast<uint32_t>(in[LAST_IDX] >> (64 - SHIFT));
	}
	if (delta > 96) {
		out[3] = static_cast<uint32_t>(in[LAST_IDX] >> (96 - SHIFT));
	}
}

// Packs for specific deltas
static void PackDelta32(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		out[i] = static_cast<uint32_t>(in[i]);
	}
}

static void PackDelta64(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		const uint8_t OFFSET = 2 * i;
		out[OFFSET] = static_cast<uint32_t>(in[i]);
		out[OFFSET + 1] = static_cast<uint32_t>(in[i] >> 32);
	}
}

static void PackDelta96(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		const uint8_t OFFSET = 3 * i;
		out[OFFSET] = static_cast<uint32_t>(in[i]);
		out[OFFSET + 1] = static_cast<uint32_t>(in[i] >> 32);
		out[OFFSET + 2] = static_cast<uint32_t>(in[i] >> 64);
	}
}

static void PackDelta128(const hugeint_t *__restrict in, uint32_t *__restrict out) {
	for (uint8_t i = 0; i < 32; ++i) {
		const uint8_t OFFSET = 4 * i;
		out[OFFSET] = static_cast<uint32_t>(in[i]);
		out[OFFSET + 1] = static_cast<uint32_t>(in[i] >> 32);
		out[OFFSET + 2] = static_cast<uint32_t>(in[i] >> 64);
		out[OFFSET + 3] = static_cast<uint32_t>(in[i] >> 96);
	}
}

//===--------------------------------------------------------------------===//
// HugeIntPacker
//===--------------------------------------------------------------------===//

void HugeIntPacker::Pack(const hugeint_t *__restrict in, uint32_t *__restrict out, bitpacking_width_t width) {
	D_ASSERT(width <= 128);
	switch (width) {
	case 0:
		break;
	case 32:
		PackDelta32(in, out);
		break;
	case 64:
		PackDelta64(in, out);
		break;
	case 96:
		PackDelta96(in, out);
		break;
	case 128:
		PackDelta128(in, out);
		break;
	default:
		for (idx_t oindex = 0; oindex < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - 1; ++oindex) {
			PackSingle(in[oindex], out, width, (width * oindex) % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE,
			           (hugeint_t(1) << width) - 1);
		}
		PackLast(in, out, width);
	}
}

void HugeIntPacker::Unpack(const uint32_t *__restrict in, hugeint_t *__restrict out, bitpacking_width_t width) {
	D_ASSERT(width <= 128);
	switch (width) {
	case 0:
		UnpackDelta0(in, out);
		break;
	case 32:
		UnpackDelta32(in, out);
		break;
	case 64:
		UnpackDelta64(in, out);
		break;
	case 96:
		UnpackDelta96(in, out);
		break;
	case 128:
		UnpackDelta128(in, out);
		break;
	default:
		for (idx_t oindex = 0; oindex < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - 1; ++oindex) {
			UnpackSingle(in, out + oindex, width,
			             (width * oindex) % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE);
		}
		UnpackLast(in, out, width);
	}
}

} // namespace duckdb


namespace duckdb {

constexpr uint8_t BitReader::REMAINDER_MASKS[];
constexpr uint8_t BitReader::MASKS[];

} // namespace duckdb









namespace duckdb {

template <class T>
CompressionFunction GetChimpFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_CHIMP, data_type, ChimpInitAnalyze<T>, ChimpAnalyze<T>,
	                           ChimpFinalAnalyze<T>, ChimpInitCompression<T>, ChimpCompress<T>,
	                           ChimpFinalizeCompress<T>, ChimpInitScan<T>, ChimpScan<T>, ChimpScanPartial<T>,
	                           ChimpFetchRow<T>, ChimpSkip<T>);
}

CompressionFunction ChimpCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
		return GetChimpFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetChimpFunction<double>(type);
	default:
		throw InternalException("Unsupported type for Chimp");
	}
}

bool ChimpCompressionFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb


namespace duckdb {

constexpr uint8_t ChimpConstants::Compression::LEADING_ROUND[];
constexpr uint8_t ChimpConstants::Compression::LEADING_REPRESENTATION[];

constexpr uint8_t ChimpConstants::Decompression::LEADING_REPRESENTATION[];

} // namespace duckdb


namespace duckdb {

constexpr uint8_t FlagBufferConstants::MASKS[];
constexpr uint8_t FlagBufferConstants::SHIFTS[];

} // namespace duckdb


namespace duckdb {

constexpr uint32_t LeadingZeroBufferConstants::MASKS[];
constexpr uint8_t LeadingZeroBufferConstants::SHIFTS[];

} // namespace duckdb












namespace duckdb {

// Abstract class for keeping compression state either for compression or size analysis
class DictionaryCompressionState : public CompressionState {
public:
	bool UpdateState(Vector &scan_vector, idx_t count) {
		UnifiedVectorFormat vdata;
		scan_vector.ToUnifiedFormat(count, vdata);
		auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
		Verify();

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			size_t string_size = 0;
			bool new_string = false;
			auto row_is_valid = vdata.validity.RowIsValid(idx);

			if (row_is_valid) {
				string_size = data[idx].GetSize();
				if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
					// Big strings not implemented for dictionary compression
					return false;
				}
				new_string = !LookupString(data[idx]);
			}

			bool fits = CalculateSpaceRequirements(new_string, string_size);
			if (!fits) {
				Flush();
				new_string = true;

				fits = CalculateSpaceRequirements(new_string, string_size);
				if (!fits) {
					throw InternalException("Dictionary compression could not write to new segment");
				}
			}

			if (!row_is_valid) {
				AddNull();
			} else if (new_string) {
				AddNewString(data[idx]);
			} else {
				AddLastLookup();
			}

			Verify();
		}

		return true;
	}

protected:
	// Should verify the State
	virtual void Verify() = 0;
	// Performs a lookup of str, storing the result internally
	virtual bool LookupString(string_t str) = 0;
	// Add the most recently looked up str to compression state
	virtual void AddLastLookup() = 0;
	// Add string to the state that is known to not be seen yet
	virtual void AddNewString(string_t str) = 0;
	// Add a null value to the compression state
	virtual void AddNull() = 0;
	// Needs to be called before adding a value. Will return false if a flush is required first.
	virtual bool CalculateSpaceRequirements(bool new_string, size_t string_size) = 0;
	// Flush the segment to disk if compressing or reset the counters if analyzing
	virtual void Flush(bool final = false) = 0;
};

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t index_buffer_offset;
	uint32_t index_buffer_count;
	uint32_t bitpacking_width;
} dictionary_compression_header_t;

struct DictionaryCompressionStorage {
	static constexpr float MINIMUM_COMPRESSION_RATIO = 1.2;
	static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);
	static constexpr size_t COMPACTION_FLUSH_LIMIT = (size_t)Storage::BLOCK_SIZE / 5 * 4;

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_DICT_VECTORS>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static bool HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);
	static idx_t RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
	                           bitpacking_width_t packing_width);

	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);
	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static string_t FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict, data_ptr_t baseptr,
	                                    int32_t dict_offset, uint16_t string_len);
	static uint16_t GetStringLength(uint32_t *index_buffer_ptr, sel_t index);
};

// Dictionary compression uses a combination of bitpacking and a dictionary to compress string segments. The data is
// stored across three buffers: the index buffer, the selection buffer and the dictionary. Firstly the Index buffer
// contains the offsets into the dictionary which are also used to determine the string lengths. Each value in the
// dictionary gets a single unique index in the index buffer. Secondly, the selection buffer maps the tuples to an index
// in the index buffer. The selection buffer is compressed with bitpacking. Finally, the dictionary contains simply all
// the unique strings without lenghts or null termination as we can deduce the lengths from the index buffer. The
// addition of the selection buffer is done for two reasons: firstly, to allow the scan to emit dictionary vectors by
// scanning the whole dictionary at once and then scanning the selection buffer for each emitted vector. Secondly, it
// allows for efficient bitpacking compression as the selection values should remain relatively small.
struct DictionaryCompressionCompressState : public DictionaryCompressionState {
	explicit DictionaryCompressionCompressState(ColumnDataCheckpointer &checkpointer_p)
	    : checkpointer(checkpointer_p),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_DICTIONARY)),
	      heap(BufferAllocator::Get(checkpointer.GetDatabase())) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	StringHeap heap;
	string_map_t<uint32_t> current_string_map;
	vector<uint32_t> index_buffer;
	vector<uint32_t> selection_buffer;

	bitpacking_width_t current_width = 0;
	bitpacking_width_t next_width = 0;

	// Result of latest LookupString call
	uint32_t latest_lookup_result;

public:
	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = std::move(compressed_segment);

		current_segment->function = function;

		// Reset the buffers and string map
		current_string_map.clear();
		index_buffer.clear();
		index_buffer.push_back(0); // Reserve index 0 for null strings
		selection_buffer.clear();

		current_width = 0;
		next_width = 0;

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = DictionaryCompressionStorage::GetDictionary(*current_segment, current_handle);
		current_end_ptr = current_handle.Ptr() + current_dictionary.end;
	}

	void Verify() override {
		current_dictionary.Verify();
		D_ASSERT(current_segment->count == selection_buffer.size());
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load(), index_buffer.size(),
		                                                      current_dictionary.size, current_width));
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);
		D_ASSERT(index_buffer.size() == current_string_map.size() + 1); // +1 is for null value
	}

	bool LookupString(string_t str) override {
		auto search = current_string_map.find(str);
		auto has_result = search != current_string_map.end();

		if (has_result) {
			latest_lookup_result = search->second;
		}
		return has_result;
	}

	void AddNewString(string_t str) override {
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, str);

		// Copy string to dict
		current_dictionary.size += str.GetSize();
		auto dict_pos = current_end_ptr - current_dictionary.size;
		memcpy(dict_pos, str.GetData(), str.GetSize());
		current_dictionary.Verify();
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// Update buffers and map
		index_buffer.push_back(current_dictionary.size);
		selection_buffer.push_back(index_buffer.size() - 1);
		if (str.IsInlined()) {
			current_string_map.insert({str, index_buffer.size() - 1});
		} else {
			current_string_map.insert({heap.AddBlob(str), index_buffer.size() - 1});
		}
		DictionaryCompressionStorage::SetDictionary(*current_segment, current_handle, current_dictionary);

		current_width = next_width;
		current_segment->count++;
	}

	void AddNull() override {
		selection_buffer.push_back(0);
		current_segment->count++;
	}

	void AddLastLookup() override {
		selection_buffer.push_back(latest_lookup_result);
		current_segment->count++;
	}

	bool CalculateSpaceRequirements(bool new_string, size_t string_size) override {
		if (new_string) {
			next_width = BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1 + new_string);
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1,
			                                                    index_buffer.size() + 1,
			                                                    current_dictionary.size + string_size, next_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_segment->count.load() + 1, index_buffer.size(),
			                                                    current_dictionary.size, current_width);
		}
	}

	void Flush(bool final = false) override {
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), segment_size);

		if (!final) {
			CreateEmptySegment(next_start);
		}
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(checkpointer.GetDatabase());
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// calculate sizes
		auto compressed_selection_buffer_size =
		    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
		auto index_buffer_size = index_buffer.size() * sizeof(uint32_t);
		auto total_size = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE + compressed_selection_buffer_size +
		                  index_buffer_size + current_dictionary.size;

		// calculate ptr and offsets
		auto base_ptr = handle.Ptr();
		auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(base_ptr);
		auto compressed_selection_buffer_offset = DictionaryCompressionStorage::DICTIONARY_HEADER_SIZE;
		auto index_buffer_offset = compressed_selection_buffer_offset + compressed_selection_buffer_size;

		// Write compressed selection buffer
		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_selection_buffer_offset,
		                                               (sel_t *)(selection_buffer.data()), current_segment->count,
		                                               current_width);

		// Write the index buffer
		memcpy(base_ptr + index_buffer_offset, index_buffer.data(), index_buffer_size);

		// Store sizes and offsets in segment header
		Store<uint32_t>(index_buffer_offset, data_ptr_cast(&header_ptr->index_buffer_offset));
		Store<uint32_t>(index_buffer.size(), data_ptr_cast(&header_ptr->index_buffer_count));
		Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));

		D_ASSERT(current_width == BitpackingPrimitives::MinimumBitWidth(index_buffer.size() - 1));
		D_ASSERT(DictionaryCompressionStorage::HasEnoughSpace(current_segment->count, index_buffer.size(),
		                                                      current_dictionary.size, current_width));
		D_ASSERT((uint64_t)*max_element(std::begin(selection_buffer), std::end(selection_buffer)) ==
		         index_buffer.size() - 1);

		if (total_size >= DictionaryCompressionStorage::COMPACTION_FLUSH_LIMIT) {
			// the block is full enough, don't bother moving around the dictionary
			return Storage::BLOCK_SIZE;
		}
		// the block has space left: figure out how much space we can save
		auto move_amount = Storage::BLOCK_SIZE - total_size;
		// move the dictionary so it lines up exactly with the offsets
		auto new_dictionary_offset = index_buffer_offset + index_buffer_size;
		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);
		// write the new dictionary (with the updated "end")
		DictionaryCompressionStorage::SetDictionary(*current_segment, handle, current_dictionary);
		return total_size;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DictionaryAnalyzeState : public DictionaryCompressionState {
	DictionaryAnalyzeState()
	    : segment_count(0), current_tuple_count(0), current_unique_count(0), current_dict_size(0), current_width(0),
	      next_width(0) {
	}

	size_t segment_count;
	idx_t current_tuple_count;
	idx_t current_unique_count;
	size_t current_dict_size;
	StringHeap heap;
	string_set_t current_set;
	bitpacking_width_t current_width;
	bitpacking_width_t next_width;

	bool LookupString(string_t str) override {
		return current_set.count(str);
	}

	void AddNewString(string_t str) override {
		current_tuple_count++;
		current_unique_count++;
		current_dict_size += str.GetSize();
		if (str.IsInlined()) {
			current_set.insert(str);
		} else {
			current_set.insert(heap.AddBlob(str));
		}
		current_width = next_width;
	}

	void AddLastLookup() override {
		current_tuple_count++;
	}

	void AddNull() override {
		current_tuple_count++;
	}

	bool CalculateSpaceRequirements(bool new_string, size_t string_size) override {
		if (new_string) {
			next_width =
			    BitpackingPrimitives::MinimumBitWidth(current_unique_count + 2); // 1 for null, one for new string
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count + 1,
			                                                    current_dict_size + string_size, next_width);
		} else {
			return DictionaryCompressionStorage::HasEnoughSpace(current_tuple_count + 1, current_unique_count,
			                                                    current_dict_size, current_width);
		}
	}

	void Flush(bool final = false) override {
		segment_count++;
		current_tuple_count = 0;
		current_unique_count = 0;
		current_dict_size = 0;
		current_set.clear();
	}
	void Verify() override {};
};

struct DictionaryCompressionAnalyzeState : public AnalyzeState {
	DictionaryCompressionAnalyzeState() : analyze_state(make_uniq<DictionaryAnalyzeState>()) {
	}

	unique_ptr<DictionaryAnalyzeState> analyze_state;
};

unique_ptr<AnalyzeState> DictionaryCompressionStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<DictionaryCompressionAnalyzeState>();
}

bool DictionaryCompressionStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<DictionaryCompressionAnalyzeState>();
	return state.analyze_state->UpdateState(input, count);
}

idx_t DictionaryCompressionStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &analyze_state = state_p.Cast<DictionaryCompressionAnalyzeState>();
	auto &state = *analyze_state.analyze_state;

	auto width = BitpackingPrimitives::MinimumBitWidth(state.current_unique_count + 1);
	auto req_space =
	    RequiredSpace(state.current_tuple_count, state.current_unique_count, state.current_dict_size, width);

	return MINIMUM_COMPRESSION_RATIO * (state.segment_count * Storage::BLOCK_SIZE + req_space);
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
unique_ptr<CompressionState> DictionaryCompressionStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                           unique_ptr<AnalyzeState> state) {
	return make_uniq<DictionaryCompressionCompressState>(checkpointer);
}

void DictionaryCompressionStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<DictionaryCompressionCompressState>();
	state.UpdateState(scan_vector, count);
}

void DictionaryCompressionStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<DictionaryCompressionCompressState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct CompressedStringScanState : public StringScanState {
	BufferHandle handle;
	buffer_ptr<Vector> dictionary;
	bitpacking_width_t current_width;
	buffer_ptr<SelectionVector> sel_vec;
	idx_t sel_vec_size = 0;
};

unique_ptr<SegmentScanState> DictionaryCompressionStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_uniq<CompressedStringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);

	auto baseptr = state->handle.Ptr() + segment.GetBlockOffset();

	// Load header values
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, state->handle);
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	auto index_buffer_count = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_count));
	state->current_width = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width)));

	auto index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);

	state->dictionary = make_buffer<Vector>(segment.type, index_buffer_count);
	auto dict_child_data = FlatVector::GetData<string_t>(*(state->dictionary));

	for (uint32_t i = 0; i < index_buffer_count; i++) {
		// NOTE: the passing of dict_child_vector, will not be used, its for big strings
		uint16_t str_len = GetStringLength(index_buffer_ptr, i);
		dict_child_data[i] = FetchStringFromDict(segment, dict, baseptr, index_buffer_ptr[i], str_len);
	}

	return std::move(state);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DICT_VECTORS>
void DictionaryCompressionStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                     Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<CompressedStringScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, scan_state.handle);

	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	auto index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);

	auto base_data = data_ptr_cast(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	if (!ALLOW_DICT_VECTORS || scan_count != STANDARD_VECTOR_SIZE ||
	    start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
		// Emit regular vector

		// Handling non-bitpacking-group-aligned start values;
		idx_t start_offset = start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		// We will scan in blocks of BITPACKING_ALGORITHM_GROUP_SIZE, so we may scan some extra values.
		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count + start_offset);

		// Create a decompression buffer of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		data_ptr_t src = &base_data[((start - start_offset) * scan_state.current_width) / 8];
		sel_t *sel_vec_ptr = scan_state.sel_vec->data();

		BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(sel_vec_ptr), src, decompress_count,
		                                          scan_state.current_width);

		for (idx_t i = 0; i < scan_count; i++) {
			// Lookup dict offset in index buffer
			auto string_number = scan_state.sel_vec->get_index(i + start_offset);
			auto dict_offset = index_buffer_ptr[string_number];
			uint16_t str_len = GetStringLength(index_buffer_ptr, string_number);
			result_data[result_offset + i] = FetchStringFromDict(segment, dict, baseptr, dict_offset, str_len);
		}

	} else {
		D_ASSERT(start % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE == 0);
		D_ASSERT(scan_count == STANDARD_VECTOR_SIZE);
		D_ASSERT(result_offset == 0);

		idx_t decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(scan_count);

		// Create a selection vector of sufficient size if we don't already have one.
		if (!scan_state.sel_vec || scan_state.sel_vec_size < decompress_count) {
			scan_state.sel_vec_size = decompress_count;
			scan_state.sel_vec = make_buffer<SelectionVector>(decompress_count);
		}

		// Scanning 1024 values, emitting a dict vector
		data_ptr_t dst = data_ptr_cast(scan_state.sel_vec->data());
		data_ptr_t src = data_ptr_cast(&base_data[(start * scan_state.current_width) / 8]);

		BitpackingPrimitives::UnPackBuffer<sel_t>(dst, src, scan_count, scan_state.current_width);

		result.Slice(*(scan_state.dictionary), *scan_state.sel_vec, scan_count);
	}
}

void DictionaryCompressionStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                              Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DictionaryCompressionStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                                  Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto &handle = state.GetOrInsertHandle(segment);

	auto baseptr = handle.Ptr() + segment.GetBlockOffset();
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(baseptr);
	auto dict = DictionaryCompressionStorage::GetDictionary(segment, handle);
	auto index_buffer_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->index_buffer_offset));
	auto width = (bitpacking_width_t)Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width));
	auto index_buffer_ptr = reinterpret_cast<uint32_t *>(baseptr + index_buffer_offset);
	auto base_data = data_ptr_cast(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	// Handling non-bitpacking-group-aligned start values;
	idx_t start_offset = row_id % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	// Decompress part of selection buffer we need for this value.
	sel_t decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];
	data_ptr_t src = data_ptr_cast(&base_data[((row_id - start_offset) * width) / 8]);
	BitpackingPrimitives::UnPackBuffer<sel_t>(data_ptr_cast(decompression_buffer), src,
	                                          BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE, width);

	auto selection_value = decompression_buffer[start_offset];
	auto dict_offset = index_buffer_ptr[selection_value];
	uint16_t str_len = GetStringLength(index_buffer_ptr, selection_value);

	result_data[result_idx] = FetchStringFromDict(segment, dict, baseptr, dict_offset, str_len);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
bool DictionaryCompressionStorage::HasEnoughSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	return RequiredSpace(current_count, index_count, dict_size, packing_width) <= Storage::BLOCK_SIZE;
}

idx_t DictionaryCompressionStorage::RequiredSpace(idx_t current_count, idx_t index_count, idx_t dict_size,
                                                  bitpacking_width_t packing_width) {
	idx_t base_space = DICTIONARY_HEADER_SIZE + dict_size;
	idx_t string_number_space = BitpackingPrimitives::GetRequiredSize(current_count, packing_width);
	idx_t index_space = index_count * sizeof(uint32_t);

	idx_t used_space = base_space + index_space + string_number_space;

	return used_space;
}

StringDictionaryContainer DictionaryCompressionStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_size));
	container.end = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_end));
	return container;
}

void DictionaryCompressionStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                                 StringDictionaryContainer container) {
	auto header_ptr = reinterpret_cast<dictionary_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	Store<uint32_t>(container.size, data_ptr_cast(&header_ptr->dict_size));
	Store<uint32_t>(container.end, data_ptr_cast(&header_ptr->dict_end));
}

string_t DictionaryCompressionStorage::FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict,
                                                           data_ptr_t baseptr, int32_t dict_offset,
                                                           uint16_t string_len) {
	D_ASSERT(dict_offset >= 0 && dict_offset <= Storage::BLOCK_SIZE);

	if (dict_offset == 0) {
		return string_t(nullptr, 0);
	}
	// normal string: read string from this block
	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;

	auto str_ptr = char_ptr_cast(dict_pos);
	return string_t(str_ptr, string_len);
}

uint16_t DictionaryCompressionStorage::GetStringLength(uint32_t *index_buffer_ptr, sel_t index) {
	if (index == 0) {
		return 0;
	} else {
		return index_buffer_ptr[index] - index_buffer_ptr[index - 1];
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DictionaryCompressionFun::GetFunction(PhysicalType data_type) {
	return CompressionFunction(
	    CompressionType::COMPRESSION_DICTIONARY, data_type, DictionaryCompressionStorage ::StringInitAnalyze,
	    DictionaryCompressionStorage::StringAnalyze, DictionaryCompressionStorage::StringFinalAnalyze,
	    DictionaryCompressionStorage::InitCompression, DictionaryCompressionStorage::Compress,
	    DictionaryCompressionStorage::FinalizeCompress, DictionaryCompressionStorage::StringInitScan,
	    DictionaryCompressionStorage::StringScan, DictionaryCompressionStorage::StringScanPartial<false>,
	    DictionaryCompressionStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DictionaryCompressionFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}
} // namespace duckdb













namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FixedSizeAnalyzeState : public AnalyzeState {
	FixedSizeAnalyzeState() : count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> FixedSizeInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<FixedSizeAnalyzeState>();
}

bool FixedSizeAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<FixedSizeAnalyzeState>();
	state.count += count;
	return true;
}

template <class T>
idx_t FixedSizeFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.template Cast<FixedSizeAnalyzeState>();
	return sizeof(T) * state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct UncompressedCompressState : public CompressionState {
	explicit UncompressedCompressState(ColumnDataCheckpointer &checkpointer);

	ColumnDataCheckpointer &checkpointer;
	unique_ptr<ColumnSegment> current_segment;
	ColumnAppendState append_state;

	virtual void CreateEmptySegment(idx_t row_start);
	void FlushSegment(idx_t segment_size);
	void Finalize(idx_t segment_size);
};

UncompressedCompressState::UncompressedCompressState(ColumnDataCheckpointer &checkpointer)
    : checkpointer(checkpointer) {
	UncompressedCompressState::CreateEmptySegment(checkpointer.GetRowGroup().start);
}

void UncompressedCompressState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpointer.GetDatabase();
	auto &type = checkpointer.GetType();
	auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
	if (type.InternalType() == PhysicalType::VARCHAR) {
		auto &state = compressed_segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		state.overflow_writer = make_uniq<WriteOverflowStringsToDisk>(checkpointer.GetRowGroup().GetBlockManager());
	}
	current_segment = std::move(compressed_segment);
	current_segment->InitializeAppend(append_state);
}

void UncompressedCompressState::FlushSegment(idx_t segment_size) {
	auto &state = checkpointer.GetCheckpointState();
	if (current_segment->type.InternalType() == PhysicalType::VARCHAR) {
		auto &segment_state = current_segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		segment_state.overflow_writer->Flush();
		segment_state.overflow_writer.reset();
	}
	state.FlushSegment(std::move(current_segment), segment_size);
}

void UncompressedCompressState::Finalize(idx_t segment_size) {
	FlushSegment(segment_size);
	current_segment.reset();
}

unique_ptr<CompressionState> UncompressedFunctions::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<UncompressedCompressState>(checkpointer);
}

void UncompressedFunctions::Compress(CompressionState &state_p, Vector &data, idx_t count) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	UnifiedVectorFormat vdata;
	data.ToUnifiedFormat(count, vdata);

	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.current_segment->Append(state.append_state, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk
		state.FlushSegment(state.current_segment->FinalizeAppend(state.append_state));

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

void UncompressedFunctions::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	state.Finalize(state.current_segment->FinalizeAppend(state.append_state));
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FixedSizeScanState : public SegmentScanState {
	BufferHandle handle;
};

unique_ptr<SegmentScanState> FixedSizeInitScan(ColumnSegment &segment) {
	auto result = make_uniq<FixedSizeScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                          idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<FixedSizeScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template <class T>
void FixedSizeScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &scan_state = state.scan_state->template Cast<FixedSizeScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, source_data);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                       idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// first fetch the data from the base table
	auto data_ptr = handle.Ptr() + segment.GetBlockOffset() + row_id * sizeof(T);

	memcpy(FlatVector::GetData(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static unique_ptr<CompressionAppendState> FixedSizeInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	return make_uniq<CompressionAppendState>(std::move(handle));
}

struct StandardFixedSizeAppend {
	template <class T>
	static void Append(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
	                   idx_t offset, idx_t count) {
		auto sdata = UnifiedVectorFormat::GetData<T>(adata);
		auto tdata = reinterpret_cast<T *>(target);
		if (!adata.validity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto source_idx = adata.sel->get_index(offset + i);
				auto target_idx = target_offset + i;
				bool is_null = !adata.validity.RowIsValid(source_idx);
				if (!is_null) {
					NumericStats::Update<T>(stats.statistics, sdata[source_idx]);
					tdata[target_idx] = sdata[source_idx];
				} else {
					// we insert a NullValue<T> in the null gap for debuggability
					// this value should never be used or read anywhere
					tdata[target_idx] = NullValue<T>();
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto source_idx = adata.sel->get_index(offset + i);
				auto target_idx = target_offset + i;
				NumericStats::Update<T>(stats.statistics, sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	}
};

struct ListFixedSizeAppend {
	template <class T>
	static void Append(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
	                   idx_t offset, idx_t count) {
		auto sdata = UnifiedVectorFormat::GetData<uint64_t>(adata);
		auto tdata = reinterpret_cast<uint64_t *>(target);
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			tdata[target_idx] = sdata[source_idx];
		}
	}
};

template <class T, class OP>
idx_t FixedSizeAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                      UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	D_ASSERT(segment.GetBlockOffset() == 0);

	auto target_ptr = append_state.handle.Ptr();
	idx_t max_tuple_count = segment.SegmentSize() / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	OP::template Append<T>(stats, target_ptr, segment.count, data, offset, copy_count);
	segment.count += copy_count;
	return copy_count;
}

template <class T>
idx_t FixedSizeFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	return segment.count * sizeof(T);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, class APPENDER = StandardFixedSizeAppend>
CompressionFunction FixedSizeGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type, FixedSizeInitAnalyze,
	                           FixedSizeAnalyze, FixedSizeFinalAnalyze<T>, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           FixedSizeInitScan, FixedSizeScan<T>, FixedSizeScanPartial<T>, FixedSizeFetchRow<T>,
	                           UncompressedFunctions::EmptySkip, nullptr, FixedSizeInitAppend,
	                           FixedSizeAppend<T, APPENDER>, FixedSizeFinalizeAppend<T>, nullptr);
}

CompressionFunction FixedSizeUncompressed::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return FixedSizeGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return FixedSizeGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return FixedSizeGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return FixedSizeGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return FixedSizeGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return FixedSizeGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return FixedSizeGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return FixedSizeGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return FixedSizeGetFunction<hugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return FixedSizeGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return FixedSizeGetFunction<double>(data_type);
	case PhysicalType::INTERVAL:
		return FixedSizeGetFunction<interval_t>(data_type);
	case PhysicalType::LIST:
		return FixedSizeGetFunction<uint64_t, ListFixedSizeAppend>(data_type);
	default:
		throw InternalException("Unsupported type for FixedSizeUncompressed::GetFunction");
	}
}

} // namespace duckdb












namespace duckdb {

typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t bitpacking_width;
	uint32_t fsst_symbol_table_offset;
} fsst_compression_header_t;

// Counts and offsets used during scanning/fetching
//                                         |               ColumnSegment to be scanned / fetched from				 |
//                                         | untouched | bp align | unused d-values | to scan | bp align | untouched |
typedef struct BPDeltaDecodeOffsets {
	idx_t delta_decode_start_row;      //                         X
	idx_t bitunpack_alignment_offset;  //			   <--------->
	idx_t bitunpack_start_row;         //	           X
	idx_t unused_delta_decoded_values; //						  <----------------->
	idx_t scan_offset;                 //			   <---------------------------->
	idx_t total_delta_decode_count;    //					      <-------------------------->
	idx_t total_bitunpack_count;       //              <------------------------------------------------>
} bp_delta_offsets_t;

struct FSSTStorage {
	static constexpr size_t COMPACTION_FLUSH_LIMIT = (size_t)Storage::BLOCK_SIZE / 5 * 4;
	static constexpr double MINIMUM_COMPRESSION_RATIO = 1.2;
	static constexpr double ANALYSIS_SAMPLE_SIZE = 0.25;

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> analyze_state_p);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_FSST_VECTORS = false>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);

	static char *FetchStringPointer(StringDictionaryContainer dict, data_ptr_t baseptr, int32_t dict_offset);
	static bp_delta_offsets_t CalculateBpDeltaOffsets(int64_t last_known_row, idx_t start, idx_t scan_count);
	static bool ParseFSSTSegmentHeader(data_ptr_t base_ptr, duckdb_fsst_decoder_t *decoder_out,
	                                   bitpacking_width_t *width_out);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FSSTAnalyzeState : public AnalyzeState {
	FSSTAnalyzeState() : count(0), fsst_string_total_size(0), empty_strings(0) {
	}

	~FSSTAnalyzeState() override {
		if (fsst_encoder) {
			duckdb_fsst_destroy(fsst_encoder);
		}
	}

	duckdb_fsst_encoder_t *fsst_encoder = nullptr;
	idx_t count;

	StringHeap fsst_string_heap;
	vector<string_t> fsst_strings;
	size_t fsst_string_total_size;

	RandomEngine random_engine;
	bool have_valid_row = false;

	idx_t empty_strings;
};

unique_ptr<AnalyzeState> FSSTStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<FSSTAnalyzeState>();
}

bool FSSTStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<FSSTAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	state.count += count;
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	// Note that we ignore the sampling in case we have not found any valid strings yet, this solves the issue of
	// not having seen any valid strings here leading to an empty fsst symbol table.
	bool sample_selected = !state.have_valid_row || state.random_engine.NextRandom() < ANALYSIS_SAMPLE_SIZE;

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}

		// We need to check all strings for this, otherwise we run in to trouble during compression if we miss ones
		auto string_size = data[idx].GetSize();
		if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
			return false;
		}

		if (!sample_selected) {
			continue;
		}

		if (string_size > 0) {
			state.have_valid_row = true;
			if (data[idx].IsInlined()) {
				state.fsst_strings.push_back(data[idx]);
			} else {
				state.fsst_strings.emplace_back(state.fsst_string_heap.AddBlob(data[idx]));
			}
			state.fsst_string_total_size += string_size;
		} else {
			state.empty_strings++;
		}
	}
	return true;
}

idx_t FSSTStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<FSSTAnalyzeState>();

	size_t compressed_dict_size = 0;
	size_t max_compressed_string_length = 0;

	auto string_count = state.fsst_strings.size();

	if (!string_count) {
		return DConstants::INVALID_INDEX;
	}

	size_t output_buffer_size = 7 + 2 * state.fsst_string_total_size; // size as specified in fsst.h

	vector<size_t> fsst_string_sizes;
	vector<unsigned char *> fsst_string_ptrs;
	for (auto &str : state.fsst_strings) {
		fsst_string_sizes.push_back(str.GetSize());
		fsst_string_ptrs.push_back((unsigned char *)str.GetData()); // NOLINT
	}

	state.fsst_encoder = duckdb_fsst_create(string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0], 0);

	// TODO: do we really need to encode to get a size estimate?
	auto compressed_ptrs = vector<unsigned char *>(string_count, nullptr);
	auto compressed_sizes = vector<size_t>(string_count, 0);
	unique_ptr<unsigned char[]> compressed_buffer(new unsigned char[output_buffer_size]);

	auto res =
	    duckdb_fsst_compress(state.fsst_encoder, string_count, &fsst_string_sizes[0], &fsst_string_ptrs[0],
	                         output_buffer_size, compressed_buffer.get(), &compressed_sizes[0], &compressed_ptrs[0]);

	if (string_count != res) {
		throw std::runtime_error("FSST output buffer is too small unexpectedly");
	}

	// Sum and and Max compressed lengths
	for (auto &size : compressed_sizes) {
		compressed_dict_size += size;
		max_compressed_string_length = MaxValue(max_compressed_string_length, size);
	}
	D_ASSERT(compressed_dict_size == (compressed_ptrs[res - 1] - compressed_ptrs[0]) + compressed_sizes[res - 1]);

	auto minimum_width = BitpackingPrimitives::MinimumBitWidth(max_compressed_string_length);
	auto bitpacked_offsets_size =
	    BitpackingPrimitives::GetRequiredSize(string_count + state.empty_strings, minimum_width);

	auto estimated_base_size = (bitpacked_offsets_size + compressed_dict_size) * (1 / ANALYSIS_SAMPLE_SIZE);
	auto num_blocks = estimated_base_size / (Storage::BLOCK_SIZE - sizeof(duckdb_fsst_decoder_t));
	auto symtable_size = num_blocks * sizeof(duckdb_fsst_decoder_t);

	auto estimated_size = estimated_base_size + symtable_size;

	return estimated_size * MINIMUM_COMPRESSION_RATIO;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//

class FSSTCompressionState : public CompressionState {
public:
	explicit FSSTCompressionState(ColumnDataCheckpointer &checkpointer)
	    : checkpointer(checkpointer), function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_FSST)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	~FSSTCompressionState() override {
		if (fsst_encoder) {
			duckdb_fsst_destroy(fsst_encoder);
		}
	}

	void Reset() {
		index_buffer.clear();
		current_width = 0;
		max_compressed_string_length = 0;
		last_fitting_size = 0;

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = FSSTStorage::GetDictionary(*current_segment, current_handle);
		current_end_ptr = current_handle.Ptr() + current_dictionary.end;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = std::move(compressed_segment);
		current_segment->function = function;
		Reset();
	}

	void UpdateState(string_t uncompressed_string, unsigned char *compressed_string, size_t compressed_string_len) {
		if (!HasEnoughSpace(compressed_string_len)) {
			Flush();
			if (!HasEnoughSpace(compressed_string_len)) {
				throw InternalException("FSST string compression failed due to insufficient space in empty block");
			};
		}

		UncompressedStringStorage::UpdateStringStats(current_segment->stats, uncompressed_string);

		// Write string into dictionary
		current_dictionary.size += compressed_string_len;
		auto dict_pos = current_end_ptr - current_dictionary.size;
		memcpy(dict_pos, compressed_string, compressed_string_len);
		current_dictionary.Verify();

		// We just push the string length to effectively delta encode the strings
		index_buffer.push_back(compressed_string_len);

		max_compressed_string_length = MaxValue(max_compressed_string_length, compressed_string_len);

		current_width = BitpackingPrimitives::MinimumBitWidth(max_compressed_string_length);
		current_segment->count++;
	}

	void AddNull() {
		if (!HasEnoughSpace(0)) {
			Flush();
			if (!HasEnoughSpace(0)) {
				throw InternalException("FSST string compression failed due to insufficient space in empty block");
			};
		}
		index_buffer.push_back(0);
		current_segment->count++;
	}

	void AddEmptyString() {
		AddNull();
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, "");
	}

	size_t GetRequiredSize(size_t string_len) {
		bitpacking_width_t required_minimum_width;
		if (string_len > max_compressed_string_length) {
			required_minimum_width = BitpackingPrimitives::MinimumBitWidth(string_len);
		} else {
			required_minimum_width = current_width;
		}

		size_t current_dict_size = current_dictionary.size;
		idx_t current_string_count = index_buffer.size();

		size_t dict_offsets_size =
		    BitpackingPrimitives::GetRequiredSize(current_string_count + 1, required_minimum_width);

		// TODO switch to a symbol table per RowGroup, saves a bit of space
		return sizeof(fsst_compression_header_t) + current_dict_size + dict_offsets_size + string_len +
		       fsst_serialized_symbol_table_size;
	}

	// Checks if there is enough space, if there is, sets last_fitting_size
	bool HasEnoughSpace(size_t string_len) {
		auto required_size = GetRequiredSize(string_len);

		if (required_size <= Storage::BLOCK_SIZE) {
			last_fitting_size = required_size;
			return true;
		}
		return false;
	}

	void Flush(bool final = false) {
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), segment_size);

		if (!final) {
			CreateEmptySegment(next_start);
		}
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// calculate sizes
		auto compressed_index_buffer_size =
		    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
		auto total_size = sizeof(fsst_compression_header_t) + compressed_index_buffer_size + current_dictionary.size +
		                  fsst_serialized_symbol_table_size;

		if (total_size != last_fitting_size) {
			throw InternalException("FSST string compression failed due to incorrect size calculation");
		}

		// calculate ptr and offsets
		auto base_ptr = handle.Ptr();
		auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(base_ptr);
		auto compressed_index_buffer_offset = sizeof(fsst_compression_header_t);
		auto symbol_table_offset = compressed_index_buffer_offset + compressed_index_buffer_size;

		D_ASSERT(current_segment->count == index_buffer.size());
		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_index_buffer_offset,
		                                               reinterpret_cast<uint32_t *>(index_buffer.data()),
		                                               current_segment->count, current_width);

		// Write the fsst symbol table or nothing
		if (fsst_encoder != nullptr) {
			memcpy(base_ptr + symbol_table_offset, &fsst_serialized_symbol_table[0], fsst_serialized_symbol_table_size);
		} else {
			memset(base_ptr + symbol_table_offset, 0, fsst_serialized_symbol_table_size);
		}

		Store<uint32_t>(symbol_table_offset, data_ptr_cast(&header_ptr->fsst_symbol_table_offset));
		Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));

		if (total_size >= FSSTStorage::COMPACTION_FLUSH_LIMIT) {
			// the block is full enough, don't bother moving around the dictionary
			return Storage::BLOCK_SIZE;
		}
		// the block has space left: figure out how much space we can save
		auto move_amount = Storage::BLOCK_SIZE - total_size;
		// move the dictionary so it lines up exactly with the offsets
		auto new_dictionary_offset = symbol_table_offset + fsst_serialized_symbol_table_size;
		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);
		// write the new dictionary (with the updated "end")
		FSSTStorage::SetDictionary(*current_segment, handle, current_dictionary);

		return total_size;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	vector<uint32_t> index_buffer;

	size_t max_compressed_string_length;
	bitpacking_width_t current_width;
	idx_t last_fitting_size;

	duckdb_fsst_encoder_t *fsst_encoder = nullptr;
	unsigned char fsst_serialized_symbol_table[sizeof(duckdb_fsst_decoder_t)];
	size_t fsst_serialized_symbol_table_size = sizeof(duckdb_fsst_decoder_t);
};

unique_ptr<CompressionState> FSSTStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                          unique_ptr<AnalyzeState> analyze_state_p) {
	auto analyze_state = static_cast<FSSTAnalyzeState *>(analyze_state_p.get());
	auto compression_state = make_uniq<FSSTCompressionState>(checkpointer);

	if (analyze_state->fsst_encoder == nullptr) {
		throw InternalException("No encoder found during FSST compression");
	}

	compression_state->fsst_encoder = analyze_state->fsst_encoder;
	compression_state->fsst_serialized_symbol_table_size =
	    duckdb_fsst_export(compression_state->fsst_encoder, &compression_state->fsst_serialized_symbol_table[0]);
	analyze_state->fsst_encoder = nullptr;

	return std::move(compression_state);
}

void FSSTStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<FSSTCompressionState>();

	// Get vector data
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	// Collect pointers to strings to compress
	vector<size_t> sizes_in;
	vector<unsigned char *> strings_in;
	size_t total_size = 0;
	idx_t total_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);

		// Note: we treat nulls and empty strings the same
		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
			continue;
		}

		total_count++;
		total_size += data[idx].GetSize();
		sizes_in.push_back(data[idx].GetSize());
		strings_in.push_back((unsigned char *)data[idx].GetData()); // NOLINT
	}

	// Only Nulls or empty strings in this vector, nothing to compress
	if (total_count == 0) {
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			if (!vdata.validity.RowIsValid(idx)) {
				state.AddNull();
			} else if (data[idx].GetSize() == 0) {
				state.AddEmptyString();
			} else {
				throw FatalException("FSST: no encoder found even though there are values to encode");
			}
		}
		return;
	}

	// Compress buffers
	size_t compress_buffer_size = MaxValue<size_t>(total_size * 2 + 7, 1);
	vector<unsigned char *> strings_out(total_count, nullptr);
	vector<size_t> sizes_out(total_count, 0);
	vector<unsigned char> compress_buffer(compress_buffer_size, 0);

	auto res = duckdb_fsst_compress(
	    state.fsst_encoder,   /* IN: encoder obtained from duckdb_fsst_create(). */
	    total_count,          /* IN: number of strings in batch to compress. */
	    &sizes_in[0],         /* IN: byte-lengths of the inputs */
	    &strings_in[0],       /* IN: input string start pointers. */
	    compress_buffer_size, /* IN: byte-length of output buffer. */
	    &compress_buffer[0],  /* OUT: memory buffer to put the compressed strings in (one after the other). */
	    &sizes_out[0],        /* OUT: byte-lengths of the compressed strings. */
	    &strings_out[0]       /* OUT: output string start pointers. Will all point into [output,output+size). */
	);

	if (res != total_count) {
		throw FatalException("FSST compression failed to compress all strings");
	}

	// Push the compressed strings to the compression state one by one
	idx_t compressed_idx = 0;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			state.AddNull();
		} else if (data[idx].GetSize() == 0) {
			state.AddEmptyString();
		} else {
			state.UpdateState(data[idx], strings_out[compressed_idx], sizes_out[compressed_idx]);
			compressed_idx++;
		}
	}
}

void FSSTStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<FSSTCompressionState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FSSTScanState : public StringScanState {
	FSSTScanState() {
		ResetStoredDelta();
	}

	buffer_ptr<void> duckdb_fsst_decoder;
	bitpacking_width_t current_width;

	// To speed up delta decoding we store the last index
	uint32_t last_known_index;
	int64_t last_known_row;

	void StoreLastDelta(uint32_t value, int64_t row) {
		last_known_index = value;
		last_known_row = row;
	}
	void ResetStoredDelta() {
		last_known_index = 0;
		last_known_row = -1;
	}
};

unique_ptr<SegmentScanState> FSSTStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_uniq<FSSTScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);
	auto base_ptr = state->handle.Ptr() + segment.GetBlockOffset();

	state->duckdb_fsst_decoder = make_buffer<duckdb_fsst_decoder_t>();
	auto retval = ParseFSSTSegmentHeader(
	    base_ptr, reinterpret_cast<duckdb_fsst_decoder_t *>(state->duckdb_fsst_decoder.get()), &state->current_width);
	if (!retval) {
		state->duckdb_fsst_decoder = nullptr;
	}

	return std::move(state);
}

void DeltaDecodeIndices(uint32_t *buffer_in, uint32_t *buffer_out, idx_t decode_count, uint32_t last_known_value) {
	buffer_out[0] = buffer_in[0];
	buffer_out[0] += last_known_value;
	for (idx_t i = 1; i < decode_count; i++) {
		buffer_out[i] = buffer_in[i] + buffer_out[i - 1];
	}
}

void BitUnpackRange(data_ptr_t src_ptr, data_ptr_t dst_ptr, idx_t count, idx_t row, bitpacking_width_t width) {
	auto bitunpack_src_ptr = &src_ptr[(row * width) / 8];
	BitpackingPrimitives::UnPackBuffer<uint32_t>(dst_ptr, bitunpack_src_ptr, count, width);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_FSST_VECTORS>
void FSSTStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {

	auto &scan_state = state.scan_state->Cast<FSSTScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	bool enable_fsst_vectors;
	if (ALLOW_FSST_VECTORS) {
		auto &config = DBConfig::GetConfig(segment.db);
		enable_fsst_vectors = config.options.enable_fsst_vectors;
	} else {
		enable_fsst_vectors = false;
	}

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, scan_state.handle);
	auto base_data = data_ptr_cast(baseptr + sizeof(fsst_compression_header_t));
	string_t *result_data;

	if (scan_count == 0) {
		return;
	}

	if (enable_fsst_vectors) {
		D_ASSERT(result_offset == 0);
		if (scan_state.duckdb_fsst_decoder) {
			D_ASSERT(result_offset == 0 || result.GetVectorType() == VectorType::FSST_VECTOR);
			result.SetVectorType(VectorType::FSST_VECTOR);
			FSSTVector::RegisterDecoder(result, scan_state.duckdb_fsst_decoder);
			result_data = FSSTVector::GetCompressedData<string_t>(result);
		} else {
			D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
			result_data = FlatVector::GetData<string_t>(result);
		}
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		result_data = FlatVector::GetData<string_t>(result);
	}

	if (start == 0 || scan_state.last_known_row >= (int64_t)start) {
		scan_state.ResetStoredDelta();
	}

	auto offsets = CalculateBpDeltaOffsets(scan_state.last_known_row, start, scan_count);

	auto bitunpack_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_bitunpack_count]);
	BitUnpackRange(base_data, data_ptr_cast(bitunpack_buffer.get()), offsets.total_bitunpack_count,
	               offsets.bitunpack_start_row, scan_state.current_width);
	auto delta_decode_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_delta_decode_count]);
	DeltaDecodeIndices(bitunpack_buffer.get() + offsets.bitunpack_alignment_offset, delta_decode_buffer.get(),
	                   offsets.total_delta_decode_count, scan_state.last_known_index);

	if (enable_fsst_vectors) {
		// Lookup decompressed offsets in dict
		for (idx_t i = 0; i < scan_count; i++) {
			uint32_t string_length = bitunpack_buffer[i + offsets.scan_offset];
			result_data[i] = UncompressedStringStorage::FetchStringFromDict(
			    segment, dict, result, baseptr, delta_decode_buffer[i + offsets.unused_delta_decoded_values],
			    string_length);
			FSSTVector::SetCount(result, scan_count);
		}
	} else {
		// Just decompress
		for (idx_t i = 0; i < scan_count; i++) {
			uint32_t str_len = bitunpack_buffer[i + offsets.scan_offset];
			auto str_ptr = FSSTStorage::FetchStringPointer(
			    dict, baseptr, delta_decode_buffer[i + offsets.unused_delta_decoded_values]);

			if (str_len > 0) {
				result_data[i + result_offset] =
				    FSSTPrimitives::DecompressValue(scan_state.duckdb_fsst_decoder.get(), result, str_ptr, str_len);
			} else {
				result_data[i + result_offset] = string_t(nullptr, 0);
			}
		}
	}

	scan_state.StoreLastDelta(delta_decode_buffer[scan_count + offsets.unused_delta_decoded_values - 1],
	                          start + scan_count - 1);
}

void FSSTStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void FSSTStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                 idx_t result_idx) {

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto base_ptr = handle.Ptr() + segment.GetBlockOffset();
	auto base_data = data_ptr_cast(base_ptr + sizeof(fsst_compression_header_t));
	auto dict = GetDictionary(segment, handle);

	duckdb_fsst_decoder_t decoder;
	bitpacking_width_t width;
	auto have_symbol_table = ParseFSSTSegmentHeader(base_ptr, &decoder, &width);

	auto result_data = FlatVector::GetData<string_t>(result);

	if (have_symbol_table) {
		// We basically just do a scan of 1 which is kinda expensive as we need to repeatedly delta decode until we
		// reach the row we want, we could consider a more clever caching trick if this is slow
		auto offsets = CalculateBpDeltaOffsets(-1, row_id, 1);

		auto bitunpack_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_bitunpack_count]);
		BitUnpackRange(base_data, data_ptr_cast(bitunpack_buffer.get()), offsets.total_bitunpack_count,
		               offsets.bitunpack_start_row, width);
		auto delta_decode_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_delta_decode_count]);
		DeltaDecodeIndices(bitunpack_buffer.get() + offsets.bitunpack_alignment_offset, delta_decode_buffer.get(),
		                   offsets.total_delta_decode_count, 0);

		uint32_t string_length = bitunpack_buffer[offsets.scan_offset];

		string_t compressed_string = UncompressedStringStorage::FetchStringFromDict(
		    segment, dict, result, base_ptr, delta_decode_buffer[offsets.unused_delta_decoded_values], string_length);

		result_data[result_idx] = FSSTPrimitives::DecompressValue((void *)&decoder, result, compressed_string.GetData(),
		                                                          compressed_string.GetSize());
	} else {
		// There's no fsst symtable, this only happens for empty strings or nulls, we can just emit an empty string
		result_data[result_idx] = string_t(nullptr, 0);
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction FSSTFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(
	    CompressionType::COMPRESSION_FSST, data_type, FSSTStorage::StringInitAnalyze, FSSTStorage::StringAnalyze,
	    FSSTStorage::StringFinalAnalyze, FSSTStorage::InitCompression, FSSTStorage::Compress,
	    FSSTStorage::FinalizeCompress, FSSTStorage::StringInitScan, FSSTStorage::StringScan,
	    FSSTStorage::StringScanPartial<false>, FSSTStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool FSSTFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
void FSSTStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container) {
	auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	Store<uint32_t>(container.size, data_ptr_cast(&header_ptr->dict_size));
	Store<uint32_t>(container.end, data_ptr_cast(&header_ptr->dict_end));
}

StringDictionaryContainer FSSTStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_size));
	container.end = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_end));
	return container;
}

char *FSSTStorage::FetchStringPointer(StringDictionaryContainer dict, data_ptr_t baseptr, int32_t dict_offset) {
	if (dict_offset == 0) {
		return nullptr;
	}

	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;
	return char_ptr_cast(dict_pos);
}

// Returns false if no symbol table was found. This means all strings are either empty or null
bool FSSTStorage::ParseFSSTSegmentHeader(data_ptr_t base_ptr, duckdb_fsst_decoder_t *decoder_out,
                                         bitpacking_width_t *width_out) {
	auto header_ptr = reinterpret_cast<fsst_compression_header_t *>(base_ptr);
	auto fsst_symbol_table_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->fsst_symbol_table_offset));
	*width_out = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width)));
	return duckdb_fsst_import(decoder_out, base_ptr + fsst_symbol_table_offset);
}

// The calculation of offsets and counts while scanning or fetching is a bit tricky, for two reasons:
// - bitunpacking needs to be aligned to BITPACKING_ALGORITHM_GROUP_SIZE
// - delta decoding needs to decode from the last known value.
bp_delta_offsets_t FSSTStorage::CalculateBpDeltaOffsets(int64_t last_known_row, idx_t start, idx_t scan_count) {
	D_ASSERT((idx_t)(last_known_row + 1) <= start);
	bp_delta_offsets_t result;

	result.delta_decode_start_row = (idx_t)(last_known_row + 1);
	result.bitunpack_alignment_offset =
	    result.delta_decode_start_row % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	result.bitunpack_start_row = result.delta_decode_start_row - result.bitunpack_alignment_offset;
	result.unused_delta_decoded_values = start - result.delta_decode_start_row;
	result.scan_offset = result.bitunpack_alignment_offset + result.unused_delta_decoded_values;
	result.total_delta_decode_count = scan_count + result.unused_delta_decoded_values;
	result.total_bitunpack_count =
	    BitpackingPrimitives::RoundUpToAlgorithmGroupSize<idx_t>(scan_count + result.scan_offset);

	D_ASSERT(result.total_delta_decode_count + result.bitunpack_alignment_offset <= result.total_bitunpack_count);
	return result;
}

} // namespace duckdb








namespace duckdb {

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> ConstantInitScan(ColumnSegment &segment) {
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Scan Partial
//===--------------------------------------------------------------------===//
void ConstantFillFunctionValidity(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &stats = segment.stats.statistics;
	if (stats.CanHaveNull()) {
		auto &mask = FlatVector::Validity(result);
		for (idx_t i = 0; i < count; i++) {
			mask.SetInvalid(start_idx + i);
		}
	}
}

template <class T>
void ConstantFillFunction(ColumnSegment &segment, Vector &result, idx_t start_idx, idx_t count) {
	auto &nstats = segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	auto constant_value = NumericStats::GetMin<T>(nstats);
	for (idx_t i = 0; i < count; i++) {
		data[start_idx + i] = constant_value;
	}
}

void ConstantScanPartialValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                 idx_t result_offset) {
	ConstantFillFunctionValidity(segment, result, result_offset, scan_count);
}

template <class T>
void ConstantScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	ConstantFillFunction<T>(segment, result, result_offset, scan_count);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ConstantScanFunctionValidity(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &stats = segment.stats.statistics;
	if (stats.CanHaveNull()) {
		if (result.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			result.Flatten(scan_count);
			ConstantFillFunctionValidity(segment, result, 0, scan_count);
		}
	}
}

template <class T>
void ConstantScanFunction(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &nstats = segment.stats.statistics;

	auto data = FlatVector::GetData<T>(result);
	data[0] = NumericStats::GetMin<T>(nstats);
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ConstantFetchRowValidity(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                              idx_t result_idx) {
	ConstantFillFunctionValidity(segment, result, result_idx, 1);
}

template <class T>
void ConstantFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	ConstantFillFunction<T>(segment, result, result_idx, 1);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ConstantGetFunctionValidity(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::BIT);
	return CompressionFunction(CompressionType::COMPRESSION_CONSTANT, data_type, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, ConstantInitScan, ConstantScanFunctionValidity,
	                           ConstantScanPartialValidity, ConstantFetchRowValidity, UncompressedFunctions::EmptySkip);
}

template <class T>
CompressionFunction ConstantGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_CONSTANT, data_type, nullptr, nullptr, nullptr, nullptr,
	                           nullptr, nullptr, ConstantInitScan, ConstantScanFunction<T>, ConstantScanPartial<T>,
	                           ConstantFetchRow<T>, UncompressedFunctions::EmptySkip);
}

CompressionFunction ConstantFun::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::BIT:
		return ConstantGetFunctionValidity(data_type);
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return ConstantGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return ConstantGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return ConstantGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return ConstantGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return ConstantGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return ConstantGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return ConstantGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return ConstantGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return ConstantGetFunction<hugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return ConstantGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return ConstantGetFunction<double>(data_type);
	default:
		throw InternalException("Unsupported type for ConstantUncompressed::GetFunction");
	}
}

bool ConstantFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BIT:
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::INT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		throw InternalException("Unsupported type for constant function");
	}
}

} // namespace duckdb

















#include <functional>

namespace duckdb {

template <class T>
CompressionFunction GetPatasFunction(PhysicalType data_type) {
	throw NotImplementedException("GetPatasFunction not implemented for the given datatype");
}

template <>
CompressionFunction GetPatasFunction<float>(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_PATAS, data_type, PatasInitAnalyze<float>,
	                           PatasAnalyze<float>, PatasFinalAnalyze<float>, PatasInitCompression<float>,
	                           PatasCompress<float>, PatasFinalizeCompress<float>, PatasInitScan<float>,
	                           PatasScan<float>, PatasScanPartial<float>, PatasFetchRow<float>, PatasSkip<float>);
}

template <>
CompressionFunction GetPatasFunction<double>(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_PATAS, data_type, PatasInitAnalyze<double>,
	                           PatasAnalyze<double>, PatasFinalAnalyze<double>, PatasInitCompression<double>,
	                           PatasCompress<double>, PatasFinalizeCompress<double>, PatasInitScan<double>,
	                           PatasScan<double>, PatasScanPartial<double>, PatasFetchRow<double>, PatasSkip<double>);
}

CompressionFunction PatasCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
		return GetPatasFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetPatasFunction<double>(type);
	default:
		throw InternalException("Unsupported type for Patas");
	}
}

bool PatasCompressionFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb









#include <functional>

namespace duckdb {

using rle_count_t = uint16_t;

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct EmptyRLEWriter {
	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr, bool is_null) {
	}
};

template <class T>
struct RLEState {
	RLEState() : seen_count(0), last_value(NullValue<T>()), last_seen_count(0), dataptr(nullptr) {
	}

	idx_t seen_count;
	T last_value;
	rle_count_t last_seen_count;
	void *dataptr;
	bool all_null = true;

public:
	template <class OP>
	void Flush() {
		OP::template Operation<T>(last_value, last_seen_count, dataptr, all_null);
	}

	template <class OP = EmptyRLEWriter>
	void Update(const T *data, ValidityMask &validity, idx_t idx) {
		if (validity.RowIsValid(idx)) {
			if (all_null) {
				// no value seen yet
				// assign the current value, and increment the seen_count
				// note that we increment last_seen_count rather than setting it to 1
				// this is intentional: this is the first VALID value we see
				// but it might not be the first value in case of nulls!
				last_value = data[idx];
				seen_count++;
				last_seen_count++;
				all_null = false;
			} else if (last_value == data[idx]) {
				// the last value is identical to this value: increment the last_seen_count
				last_seen_count++;
			} else {
				// the values are different
				// issue the callback on the last value
				Flush<OP>();

				// increment the seen_count and put the new value into the RLE slot
				last_value = data[idx];
				seen_count++;
				last_seen_count = 1;
			}
		} else {
			// NULL value: we merely increment the last_seen_count
			last_seen_count++;
		}
		if (last_seen_count == NumericLimits<rle_count_t>::Maximum()) {
			// we have seen the same value so many times in a row we are at the limit of what fits in our count
			// write away the value and move to the next value
			Flush<OP>();
			last_seen_count = 0;
			seen_count++;
		}
	}
};

template <class T>
struct RLEAnalyzeState : public AnalyzeState {
	RLEAnalyzeState() {
	}

	RLEState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> RLEInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<RLEAnalyzeState<T>>();
}

template <class T>
bool RLEAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &rle_state = state.template Cast<RLEAnalyzeState<T>>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		rle_state.state.Update(data, vdata.validity, idx);
	}
	return true;
}

template <class T>
idx_t RLEFinalAnalyze(AnalyzeState &state) {
	auto &rle_state = state.template Cast<RLEAnalyzeState<T>>();
	return (sizeof(rle_count_t) + sizeof(T)) * rle_state.state.seen_count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct RLEConstants {
	static constexpr const idx_t RLE_HEADER_SIZE = sizeof(uint64_t);
};

template <class T, bool WRITE_STATISTICS>
struct RLECompressState : public CompressionState {
	struct RLEWriter {
		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr, bool is_null) {
			auto state = reinterpret_cast<RLECompressState<T, WRITE_STATISTICS> *>(dataptr);
			state->WriteValue(value, count, is_null);
		}
	};

	static idx_t MaxRLECount() {
		auto entry_size = sizeof(T) + sizeof(rle_count_t);
		auto entry_count = (Storage::BLOCK_SIZE - RLEConstants::RLE_HEADER_SIZE) / entry_size;
		auto max_vector_count = entry_count / STANDARD_VECTOR_SIZE;
		return max_vector_count * STANDARD_VECTOR_SIZE;
	}

	explicit RLECompressState(ColumnDataCheckpointer &checkpointer_p)
	    : checkpointer(checkpointer_p),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_RLE)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.dataptr = (void *)this;
		max_rle_count = MaxRLECount();
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto column_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		column_segment->function = function;
		current_segment = std::move(column_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = UnifiedVectorFormat::GetData<T>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<RLECompressState<T, WRITE_STATISTICS>::RLEWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValue(T value, rle_count_t count, bool is_null) {
		// write the RLE entry
		auto handle_ptr = handle.Ptr() + RLEConstants::RLE_HEADER_SIZE;
		auto data_pointer = reinterpret_cast<T *>(handle_ptr);
		auto index_pointer = reinterpret_cast<rle_count_t *>(handle_ptr + max_rle_count * sizeof(T));
		data_pointer[entry_count] = value;
		index_pointer[entry_count] = count;
		entry_count++;

		// update meta data
		if (WRITE_STATISTICS && !is_null) {
			NumericStats::Update<T>(current_segment->stats.statistics, value);
		}
		current_segment->count += count;

		if (entry_count == max_rle_count) {
			// we have finished writing this segment: flush it and create a new segment
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
			entry_count = 0;
		}
	}

	void FlushSegment() {
		// flush the segment
		// we compact the segment by moving the counts so they are directly next to the values
		idx_t counts_size = sizeof(rle_count_t) * entry_count;
		idx_t original_rle_offset = RLEConstants::RLE_HEADER_SIZE + max_rle_count * sizeof(T);
		idx_t minimal_rle_offset = AlignValue(RLEConstants::RLE_HEADER_SIZE + sizeof(T) * entry_count);
		idx_t total_segment_size = minimal_rle_offset + counts_size;
		auto data_ptr = handle.Ptr();
		memmove(data_ptr + minimal_rle_offset, data_ptr + original_rle_offset, counts_size);
		// store the final RLE offset within the segment
		Store<uint64_t>(minimal_rle_offset, data_ptr);
		handle.Destroy();

		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<RLECompressState<T, WRITE_STATISTICS>::RLEWriter>();

		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	RLEState<T> state;
	idx_t entry_count = 0;
	idx_t max_rle_count;
};

template <class T, bool WRITE_STATISTICS>
unique_ptr<CompressionState> RLEInitCompression(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> state) {
	return make_uniq<RLECompressState<T, WRITE_STATISTICS>>(checkpointer);
}

template <class T, bool WRITE_STATISTICS>
void RLECompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (RLECompressState<T, WRITE_STATISTICS> &)state_p;
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);

	state.Append(vdata, count);
}

template <class T, bool WRITE_STATISTICS>
void RLEFinalizeCompress(CompressionState &state_p) {
	auto &state = (RLECompressState<T, WRITE_STATISTICS> &)state_p;
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
struct RLEScanState : public SegmentScanState {
	explicit RLEScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		entry_pos = 0;
		position_in_entry = 0;
		rle_count_offset = Load<uint64_t>(handle.Ptr() + segment.GetBlockOffset());
		D_ASSERT(rle_count_offset <= Storage::BLOCK_SIZE);
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		auto data = handle.Ptr() + segment.GetBlockOffset();
		auto index_pointer = reinterpret_cast<rle_count_t *>(data + rle_count_offset);

		for (idx_t i = 0; i < skip_count; i++) {
			// assign the current value
			position_in_entry++;
			if (position_in_entry >= index_pointer[entry_pos]) {
				// handled all entries in this RLE value
				// move to the next entry
				entry_pos++;
				position_in_entry = 0;
			}
		}
	}

	BufferHandle handle;
	idx_t entry_pos;
	idx_t position_in_entry;
	uint32_t rle_count_offset;
};

template <class T>
unique_ptr<SegmentScanState> RLEInitScan(ColumnSegment &segment) {
	auto result = make_uniq<RLEScanState<T>>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void RLESkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = state.scan_state->Cast<RLEScanState<T>>();
	scan_state.Skip(segment, skip_count);
}

template <bool ENTIRE_VECTOR>
static bool CanEmitConstantVector(idx_t position, idx_t run_length, idx_t scan_count) {
	if (!ENTIRE_VECTOR) {
		return false;
	}
	if (scan_count != STANDARD_VECTOR_SIZE) {
		// Only when we can fill an entire Vector can we emit a ConstantVector, because subsequent scans require the
		// input Vector to be flat
		return false;
	}
	D_ASSERT(position < run_length);
	auto remaining_in_run = run_length - position;
	// The amount of values left in this run are equal or greater than the amount of values we need to scan
	return remaining_in_run >= scan_count;
}

template <class T>
inline static void ForwardToNextRun(RLEScanState<T> &scan_state) {
	// handled all entries in this RLE value
	// move to the next entry
	scan_state.entry_pos++;
	scan_state.position_in_entry = 0;
}

template <class T>
inline static bool ExhaustedRun(RLEScanState<T> &scan_state, rle_count_t *index_pointer) {
	return scan_state.position_in_entry >= index_pointer[scan_state.entry_pos];
}

template <class T>
static void RLEScanConstant(RLEScanState<T> &scan_state, rle_count_t *index_pointer, T *data_pointer, idx_t scan_count,
                            Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<T>(result);
	result_data[0] = data_pointer[scan_state.entry_pos];
	scan_state.position_in_entry += scan_count;
	if (ExhaustedRun(scan_state, index_pointer)) {
		ForwardToNextRun(scan_state);
	}
	return;
}

template <class T, bool ENTIRE_VECTOR>
void RLEScanPartialInternal(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                            idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<RLEScanState<T>>();

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = reinterpret_cast<T *>(data + RLEConstants::RLE_HEADER_SIZE);
	auto index_pointer = reinterpret_cast<rle_count_t *>(data + scan_state.rle_count_offset);

	// If we are scanning an entire Vector and it contains only a single run
	if (CanEmitConstantVector<ENTIRE_VECTOR>(scan_state.position_in_entry, index_pointer[scan_state.entry_pos],
	                                         scan_count)) {
		RLEScanConstant<T>(scan_state, index_pointer, data_pointer, scan_count, result);
		return;
	}

	auto result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < scan_count; i++) {
		// assign the current value
		result_data[result_offset + i] = data_pointer[scan_state.entry_pos];
		scan_state.position_in_entry++;
		if (ExhaustedRun(scan_state, index_pointer)) {
			ForwardToNextRun(scan_state);
		}
	}
}

template <class T>
void RLEScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                    idx_t result_offset) {
	return RLEScanPartialInternal<T, false>(segment, state, scan_count, result, result_offset);
}

template <class T>
void RLEScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	RLEScanPartialInternal<T, true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void RLEFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RLEScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = reinterpret_cast<T *>(data + RLEConstants::RLE_HEADER_SIZE);
	auto result_data = FlatVector::GetData<T>(result);
	result_data[result_idx] = data_pointer[scan_state.entry_pos];
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS = true>
CompressionFunction GetRLEFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_RLE, data_type, RLEInitAnalyze<T>, RLEAnalyze<T>,
	                           RLEFinalAnalyze<T>, RLEInitCompression<T, WRITE_STATISTICS>,
	                           RLECompress<T, WRITE_STATISTICS>, RLEFinalizeCompress<T, WRITE_STATISTICS>,
	                           RLEInitScan<T>, RLEScan<T>, RLEScanPartial<T>, RLEFetchRow<T>, RLESkip<T>);
}

CompressionFunction RLEFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetRLEFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetRLEFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetRLEFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetRLEFunction<int64_t>(type);
	case PhysicalType::INT128:
		return GetRLEFunction<hugeint_t>(type);
	case PhysicalType::UINT8:
		return GetRLEFunction<uint8_t>(type);
	case PhysicalType::UINT16:
		return GetRLEFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetRLEFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetRLEFunction<uint64_t>(type);
	case PhysicalType::FLOAT:
		return GetRLEFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetRLEFunction<double>(type);
	case PhysicalType::LIST:
		return GetRLEFunction<uint64_t, false>(type);
	default:
		throw InternalException("Unsupported type for RLE");
	}
}

bool RLEFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::LIST:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb








namespace duckdb {

//===--------------------------------------------------------------------===//
// Storage Class
//===--------------------------------------------------------------------===//
UncompressedStringSegmentState::~UncompressedStringSegmentState() {
	while (head) {
		// prevent deep recursion here
		head = std::move(head->next);
	}
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct StringAnalyzeState : public AnalyzeState {
	StringAnalyzeState() : count(0), total_string_size(0), overflow_strings(0) {
	}

	idx_t count;
	idx_t total_string_size;
	idx_t overflow_strings;
};

unique_ptr<AnalyzeState> UncompressedStringStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<StringAnalyzeState>();
}

bool UncompressedStringStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<StringAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	state.count += count;
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			auto string_size = data[idx].GetSize();
			state.total_string_size += string_size;
			if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
				state.overflow_strings++;
			}
		}
	}
	return true;
}

idx_t UncompressedStringStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<StringAnalyzeState>();
	return state.count * sizeof(int32_t) + state.total_string_size + state.overflow_strings * BIG_STRING_MARKER_SIZE;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
unique_ptr<SegmentScanState> UncompressedStringStorage::StringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<StringScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                                  Vector &result, idx_t result_offset) {
	// clear any previously locked buffers and get the primary buffer handle
	auto &scan_state = state.scan_state->Cast<StringScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, scan_state.handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	int32_t previous_offset = start > 0 ? base_data[start - 1] : 0;

	for (idx_t i = 0; i < scan_count; i++) {
		// std::abs used since offsets can be negative to indicate big strings
		uint32_t string_length = std::abs(base_data[start + i]) - std::abs(previous_offset);
		result_data[result_offset + i] =
		    FetchStringFromDict(segment, dict, result, baseptr, base_data[start + i], string_length);
		previous_offset = base_data[start + i];
	}
}

void UncompressedStringStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                           Vector &result) {
	StringScanPartial(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
BufferHandle &ColumnFetchState::GetOrInsertHandle(ColumnSegment &segment) {
	auto primary_id = segment.block->BlockId();

	auto entry = handles.find(primary_id);
	if (entry == handles.end()) {
		// not pinned yet: pin it
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		auto handle = buffer_manager.Pin(segment.block);
		auto entry = handles.insert(make_pair(primary_id, std::move(handle)));
		return entry.first->second;
	} else {
		// already pinned: use the pinned handle
		return entry->second;
	}
}

void UncompressedStringStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id,
                                               Vector &result, idx_t result_idx) {
	// fetch a single row from the string segment
	// first pin the main buffer if it is not already pinned
	auto &handle = state.GetOrInsertHandle(segment);

	auto baseptr = handle.Ptr() + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, handle);
	auto base_data = reinterpret_cast<int32_t *>(baseptr + DICTIONARY_HEADER_SIZE);
	auto result_data = FlatVector::GetData<string_t>(result);

	auto dict_offset = base_data[row_id];
	uint32_t string_length;
	if ((idx_t)row_id == 0) {
		// edge case where this is the first string in the dict
		string_length = std::abs(dict_offset);
	} else {
		string_length = std::abs(dict_offset) - std::abs(base_data[row_id - 1]);
	}
	result_data[result_idx] = FetchStringFromDict(segment, dict, result, baseptr, dict_offset, string_length);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
struct SerializedStringSegmentState : public ColumnSegmentState {
	SerializedStringSegmentState() {
	}
	explicit SerializedStringSegmentState(vector<block_id_t> blocks_p) : blocks(std::move(blocks_p)) {
	}

	vector<block_id_t> blocks;

	void Serialize(Serializer &serializer) const override {
		serializer.WriteProperty(1, "overflow_blocks", blocks);
	}
};

unique_ptr<CompressedSegmentState>
UncompressedStringStorage::StringInitSegment(ColumnSegment &segment, block_id_t block_id,
                                             optional_ptr<ColumnSegmentState> segment_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.block);
		StringDictionaryContainer dictionary;
		dictionary.size = 0;
		dictionary.end = segment.SegmentSize();
		SetDictionary(segment, handle, dictionary);
	}
	auto result = make_uniq<UncompressedStringSegmentState>();
	if (segment_state) {
		auto &serialized_state = segment_state->Cast<SerializedStringSegmentState>();
		result->on_disk_blocks = std::move(serialized_state.blocks);
	}
	return std::move(result);
}

idx_t UncompressedStringStorage::FinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dict = GetDictionary(segment, handle);
	D_ASSERT(dict.end == segment.SegmentSize());
	// compute the total size required to store this segment
	auto offset_size = DICTIONARY_HEADER_SIZE + segment.count * sizeof(int32_t);
	auto total_size = offset_size + dict.size;
	if (total_size >= COMPACTION_FLUSH_LIMIT) {
		// the block is full enough, don't bother moving around the dictionary
		return segment.SegmentSize();
	}
	// the block has space left: figure out how much space we can save
	auto move_amount = segment.SegmentSize() - total_size;
	// move the dictionary so it lines up exactly with the offsets
	auto dataptr = handle.Ptr();
	memmove(dataptr + offset_size, dataptr + dict.end - dict.size, dict.size);
	dict.end -= move_amount;
	D_ASSERT(dict.end == total_size);
	// write the new dictionary (with the updated "end")
	SetDictionary(segment, handle, dict);
	return total_size;
}

//===--------------------------------------------------------------------===//
// Serialization & Cleanup
//===--------------------------------------------------------------------===//
unique_ptr<ColumnSegmentState> UncompressedStringStorage::SerializeState(ColumnSegment &segment) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (state.on_disk_blocks.empty()) {
		// no on-disk blocks - nothing to write
		return nullptr;
	}
	return make_uniq<SerializedStringSegmentState>(state.on_disk_blocks);
}

unique_ptr<ColumnSegmentState> UncompressedStringStorage::DeserializeState(Deserializer &deserializer) {
	auto result = make_uniq<SerializedStringSegmentState>();
	deserializer.ReadProperty(1, "overflow_blocks", result->blocks);
	return std::move(result);
}

void UncompressedStringStorage::CleanupState(ColumnSegment &segment) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	auto &block_manager = segment.GetBlockManager();
	for (auto &block_id : state.on_disk_blocks) {
		block_manager.MarkBlockAsModified(block_id);
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction StringUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type,
	                           UncompressedStringStorage::StringInitAnalyze, UncompressedStringStorage::StringAnalyze,
	                           UncompressedStringStorage::StringFinalAnalyze, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           UncompressedStringStorage::StringInitScan, UncompressedStringStorage::StringScan,
	                           UncompressedStringStorage::StringScanPartial, UncompressedStringStorage::StringFetchRow,
	                           UncompressedFunctions::EmptySkip, UncompressedStringStorage::StringInitSegment,
	                           UncompressedStringStorage::StringInitAppend, UncompressedStringStorage::StringAppend,
	                           UncompressedStringStorage::FinalizeAppend, nullptr,
	                           UncompressedStringStorage::SerializeState, UncompressedStringStorage::DeserializeState,
	                           UncompressedStringStorage::CleanupState);
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
void UncompressedStringStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle,
                                              StringDictionaryContainer container) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	Store<uint32_t>(container.size, startptr);
	Store<uint32_t>(container.end, startptr + sizeof(uint32_t));
}

StringDictionaryContainer UncompressedStringStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto startptr = handle.Ptr() + segment.GetBlockOffset();
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(startptr);
	container.end = Load<uint32_t>(startptr + sizeof(uint32_t));
	return container;
}

idx_t UncompressedStringStorage::RemainingSpace(ColumnSegment &segment, BufferHandle &handle) {
	auto dictionary = GetDictionary(segment, handle);
	D_ASSERT(dictionary.end == segment.SegmentSize());
	idx_t used_space = dictionary.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
	D_ASSERT(segment.SegmentSize() >= used_space);
	return segment.SegmentSize() - used_space;
}

void UncompressedStringStorage::WriteString(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                            int32_t &result_offset) {
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (state.overflow_writer) {
		// overflow writer is set: write string there
		state.overflow_writer->WriteString(state, string, result_block, result_offset);
	} else {
		// default overflow behavior: use in-memory buffer to store the overflow string
		WriteStringMemory(segment, string, result_block, result_offset);
	}
}

void UncompressedStringStorage::WriteStringMemory(ColumnSegment &segment, string_t string, block_id_t &result_block,
                                                  int32_t &result_offset) {
	uint32_t total_length = string.GetSize() + sizeof(uint32_t);
	shared_ptr<BlockHandle> block;
	BufferHandle handle;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	// check if the string fits in the current block
	if (!state.head || state.head->offset + total_length >= state.head->size) {
		// string does not fit, allocate space for it
		// create a new string block
		idx_t alloc_size = MaxValue<idx_t>(total_length, Storage::BLOCK_SIZE);
		auto new_block = make_uniq<StringBlock>();
		new_block->offset = 0;
		new_block->size = alloc_size;
		// allocate an in-memory buffer for it
		handle = buffer_manager.Allocate(alloc_size, false, &block);
		state.overflow_blocks.insert(make_pair(block->BlockId(), reference<StringBlock>(*new_block)));
		new_block->block = std::move(block);
		new_block->next = std::move(state.head);
		state.head = std::move(new_block);
	} else {
		// string fits, copy it into the current block
		handle = buffer_manager.Pin(state.head->block);
	}

	result_block = state.head->block->BlockId();
	result_offset = state.head->offset;

	// copy the string and the length there
	auto ptr = handle.Ptr() + state.head->offset;
	Store<uint32_t>(string.GetSize(), ptr);
	ptr += sizeof(uint32_t);
	memcpy(ptr, string.GetData(), string.GetSize());
	state.head->offset += total_length;
}

string_t UncompressedStringStorage::ReadOverflowString(ColumnSegment &segment, Vector &result, block_id_t block,
                                                       int32_t offset) {
	D_ASSERT(block != INVALID_BLOCK);
	D_ASSERT(offset < Storage::BLOCK_SIZE);

	auto &block_manager = segment.GetBlockManager();
	auto &buffer_manager = block_manager.buffer_manager;
	auto &state = segment.GetSegmentState()->Cast<UncompressedStringSegmentState>();
	if (block < MAXIMUM_BLOCK) {
		// read the overflow string from disk
		// pin the initial handle and read the length
		auto block_handle = state.GetHandle(block_manager, block);
		auto handle = buffer_manager.Pin(block_handle);

		// read header
		uint32_t length = Load<uint32_t>(handle.Ptr() + offset);
		uint32_t remaining = length;
		offset += sizeof(uint32_t);

		// allocate a buffer to store the string
		auto alloc_size = MaxValue<idx_t>(Storage::BLOCK_SIZE, length);
		// allocate a buffer to store the compressed string
		// TODO: profile this to check if we need to reuse buffer
		auto target_handle = buffer_manager.Allocate(alloc_size);
		auto target_ptr = target_handle.Ptr();

		// now append the string to the single buffer
		while (remaining > 0) {
			idx_t to_write = MinValue<idx_t>(remaining, Storage::BLOCK_SIZE - sizeof(block_id_t) - offset);
			memcpy(target_ptr, handle.Ptr() + offset, to_write);
			remaining -= to_write;
			offset += to_write;
			target_ptr += to_write;
			if (remaining > 0) {
				// read the next block
				block_id_t next_block = Load<block_id_t>(handle.Ptr() + offset);
				block_handle = state.GetHandle(block_manager, next_block);
				handle = buffer_manager.Pin(block_handle);
				offset = 0;
			}
		}

		auto final_buffer = target_handle.Ptr();
		StringVector::AddHandle(result, std::move(target_handle));
		return ReadString(final_buffer, 0, length);
	} else {
		// read the overflow string from memory
		// first pin the handle, if it is not pinned yet
		auto entry = state.overflow_blocks.find(block);
		D_ASSERT(entry != state.overflow_blocks.end());
		auto handle = buffer_manager.Pin(entry->second.get().block);
		auto final_buffer = handle.Ptr();
		StringVector::AddHandle(result, std::move(handle));
		return ReadStringWithLength(final_buffer, offset);
	}
}

string_t UncompressedStringStorage::ReadString(data_ptr_t target, int32_t offset, uint32_t string_length) {
	auto ptr = target + offset;
	auto str_ptr = char_ptr_cast(ptr);
	return string_t(str_ptr, string_length);
}

string_t UncompressedStringStorage::ReadStringWithLength(data_ptr_t target, int32_t offset) {
	auto ptr = target + offset;
	auto str_length = Load<uint32_t>(ptr);
	auto str_ptr = char_ptr_cast(ptr + sizeof(uint32_t));
	return string_t(str_ptr, str_length);
}

void UncompressedStringStorage::WriteStringMarker(data_ptr_t target, block_id_t block_id, int32_t offset) {
	memcpy(target, &block_id, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(target, &offset, sizeof(int32_t));
}

void UncompressedStringStorage::ReadStringMarker(data_ptr_t target, block_id_t &block_id, int32_t &offset) {
	memcpy(&block_id, target, sizeof(block_id_t));
	target += sizeof(block_id_t);
	memcpy(&offset, target, sizeof(int32_t));
}

string_location_t UncompressedStringStorage::FetchStringLocation(StringDictionaryContainer dict, data_ptr_t baseptr,
                                                                 int32_t dict_offset) {
	D_ASSERT(dict_offset >= -1 * Storage::BLOCK_SIZE && dict_offset <= Storage::BLOCK_SIZE);
	if (dict_offset < 0) {
		string_location_t result;
		ReadStringMarker(baseptr + dict.end - (-1 * dict_offset), result.block_id, result.offset);
		return result;
	} else {
		return string_location_t(INVALID_BLOCK, dict_offset);
	}
}

string_t UncompressedStringStorage::FetchStringFromDict(ColumnSegment &segment, StringDictionaryContainer dict,
                                                        Vector &result, data_ptr_t baseptr, int32_t dict_offset,
                                                        uint32_t string_length) {
	// fetch base data
	D_ASSERT(dict_offset <= Storage::BLOCK_SIZE);
	string_location_t location = FetchStringLocation(dict, baseptr, dict_offset);
	return FetchString(segment, dict, result, baseptr, location, string_length);
}

string_t UncompressedStringStorage::FetchString(ColumnSegment &segment, StringDictionaryContainer dict, Vector &result,
                                                data_ptr_t baseptr, string_location_t location,
                                                uint32_t string_length) {
	if (location.block_id != INVALID_BLOCK) {
		// big string marker: read from separate block
		return ReadOverflowString(segment, result, location.block_id, location.offset);
	} else {
		if (location.offset == 0) {
			return string_t(nullptr, 0);
		}
		// normal string: read string from this block
		auto dict_end = baseptr + dict.end;
		auto dict_pos = dict_end - location.offset;

		auto str_ptr = char_ptr_cast(dict_pos);
		return string_t(str_ptr, string_length);
	}
}

} // namespace duckdb



namespace duckdb {

CompressionFunction UncompressedFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::LIST:
	case PhysicalType::INTERVAL:
		return FixedSizeUncompressed::GetFunction(type);
	case PhysicalType::BIT:
		return ValidityUncompressed::GetFunction(type);
	case PhysicalType::VARCHAR:
		return StringUncompressed::GetFunction(type);
	default:
		throw InternalException("Unsupported type for Uncompressed");
	}
}

bool UncompressedFun::TypeIsSupported(PhysicalType type) {
	return true;
}

} // namespace duckdb










namespace duckdb {

//===--------------------------------------------------------------------===//
// Mask constants
//===--------------------------------------------------------------------===//
// LOWER_MASKS contains masks with all the lower bits set until a specific value
// LOWER_MASKS[0] has the 0 lowest bits set, i.e.:
// 0b0000000000000000000000000000000000000000000000000000000000000000,
// LOWER_MASKS[10] has the 10 lowest bits set, i.e.:
// 0b0000000000000000000000000000000000000000000000000000000111111111,
// etc...
// 0b0000000000000000000000000000000000000001111111111111111111111111,
// ...
// 0b0000000000000000000001111111111111111111111111111111111111111111,
// until LOWER_MASKS[64], which has all bits set:
// 0b1111111111111111111111111111111111111111111111111111111111111111
// generated with this python snippet:
// for i in range(65):
//   print(hex(int((64 - i) * '0' + i * '1', 2)) + ",")
const validity_t ValidityUncompressed::LOWER_MASKS[] = {0x0,
                                                        0x1,
                                                        0x3,
                                                        0x7,
                                                        0xf,
                                                        0x1f,
                                                        0x3f,
                                                        0x7f,
                                                        0xff,
                                                        0x1ff,
                                                        0x3ff,
                                                        0x7ff,
                                                        0xfff,
                                                        0x1fff,
                                                        0x3fff,
                                                        0x7fff,
                                                        0xffff,
                                                        0x1ffff,
                                                        0x3ffff,
                                                        0x7ffff,
                                                        0xfffff,
                                                        0x1fffff,
                                                        0x3fffff,
                                                        0x7fffff,
                                                        0xffffff,
                                                        0x1ffffff,
                                                        0x3ffffff,
                                                        0x7ffffff,
                                                        0xfffffff,
                                                        0x1fffffff,
                                                        0x3fffffff,
                                                        0x7fffffff,
                                                        0xffffffff,
                                                        0x1ffffffff,
                                                        0x3ffffffff,
                                                        0x7ffffffff,
                                                        0xfffffffff,
                                                        0x1fffffffff,
                                                        0x3fffffffff,
                                                        0x7fffffffff,
                                                        0xffffffffff,
                                                        0x1ffffffffff,
                                                        0x3ffffffffff,
                                                        0x7ffffffffff,
                                                        0xfffffffffff,
                                                        0x1fffffffffff,
                                                        0x3fffffffffff,
                                                        0x7fffffffffff,
                                                        0xffffffffffff,
                                                        0x1ffffffffffff,
                                                        0x3ffffffffffff,
                                                        0x7ffffffffffff,
                                                        0xfffffffffffff,
                                                        0x1fffffffffffff,
                                                        0x3fffffffffffff,
                                                        0x7fffffffffffff,
                                                        0xffffffffffffff,
                                                        0x1ffffffffffffff,
                                                        0x3ffffffffffffff,
                                                        0x7ffffffffffffff,
                                                        0xfffffffffffffff,
                                                        0x1fffffffffffffff,
                                                        0x3fffffffffffffff,
                                                        0x7fffffffffffffff,
                                                        0xffffffffffffffff};

// UPPER_MASKS contains masks with all the highest bits set until a specific value
// UPPER_MASKS[0] has the 0 highest bits set, i.e.:
// 0b0000000000000000000000000000000000000000000000000000000000000000,
// UPPER_MASKS[10] has the 10 highest bits set, i.e.:
// 0b1111111111110000000000000000000000000000000000000000000000000000,
// etc...
// 0b1111111111111111111111110000000000000000000000000000000000000000,
// ...
// 0b1111111111111111111111111111111111111110000000000000000000000000,
// until UPPER_MASKS[64], which has all bits set:
// 0b1111111111111111111111111111111111111111111111111111111111111111
// generated with this python snippet:
// for i in range(65):
//   print(hex(int(i * '1' + (64 - i) * '0', 2)) + ",")
const validity_t ValidityUncompressed::UPPER_MASKS[] = {0x0,
                                                        0x8000000000000000,
                                                        0xc000000000000000,
                                                        0xe000000000000000,
                                                        0xf000000000000000,
                                                        0xf800000000000000,
                                                        0xfc00000000000000,
                                                        0xfe00000000000000,
                                                        0xff00000000000000,
                                                        0xff80000000000000,
                                                        0xffc0000000000000,
                                                        0xffe0000000000000,
                                                        0xfff0000000000000,
                                                        0xfff8000000000000,
                                                        0xfffc000000000000,
                                                        0xfffe000000000000,
                                                        0xffff000000000000,
                                                        0xffff800000000000,
                                                        0xffffc00000000000,
                                                        0xffffe00000000000,
                                                        0xfffff00000000000,
                                                        0xfffff80000000000,
                                                        0xfffffc0000000000,
                                                        0xfffffe0000000000,
                                                        0xffffff0000000000,
                                                        0xffffff8000000000,
                                                        0xffffffc000000000,
                                                        0xffffffe000000000,
                                                        0xfffffff000000000,
                                                        0xfffffff800000000,
                                                        0xfffffffc00000000,
                                                        0xfffffffe00000000,
                                                        0xffffffff00000000,
                                                        0xffffffff80000000,
                                                        0xffffffffc0000000,
                                                        0xffffffffe0000000,
                                                        0xfffffffff0000000,
                                                        0xfffffffff8000000,
                                                        0xfffffffffc000000,
                                                        0xfffffffffe000000,
                                                        0xffffffffff000000,
                                                        0xffffffffff800000,
                                                        0xffffffffffc00000,
                                                        0xffffffffffe00000,
                                                        0xfffffffffff00000,
                                                        0xfffffffffff80000,
                                                        0xfffffffffffc0000,
                                                        0xfffffffffffe0000,
                                                        0xffffffffffff0000,
                                                        0xffffffffffff8000,
                                                        0xffffffffffffc000,
                                                        0xffffffffffffe000,
                                                        0xfffffffffffff000,
                                                        0xfffffffffffff800,
                                                        0xfffffffffffffc00,
                                                        0xfffffffffffffe00,
                                                        0xffffffffffffff00,
                                                        0xffffffffffffff80,
                                                        0xffffffffffffffc0,
                                                        0xffffffffffffffe0,
                                                        0xfffffffffffffff0,
                                                        0xfffffffffffffff8,
                                                        0xfffffffffffffffc,
                                                        0xfffffffffffffffe,
                                                        0xffffffffffffffff};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct ValidityAnalyzeState : public AnalyzeState {
	ValidityAnalyzeState() : count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> ValidityInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<ValidityAnalyzeState>();
}

bool ValidityAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<ValidityAnalyzeState>();
	state.count += count;
	return true;
}

idx_t ValidityFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<ValidityAnalyzeState>();
	return (state.count + 7) / 8;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct ValidityScanState : public SegmentScanState {
	BufferHandle handle;
	block_id_t block_id;
};

unique_ptr<SegmentScanState> ValidityInitScan(ColumnSegment &segment) {
	auto result = make_uniq<ValidityScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	result->block_id = segment.block->BlockId();
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
void ValidityScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                         idx_t result_offset) {
	auto start = segment.GetRelativeIndex(state.row_index);

	static_assert(sizeof(validity_t) == sizeof(uint64_t), "validity_t should be 64-bit");
	auto &scan_state = state.scan_state->Cast<ValidityScanState>();

	auto &result_mask = FlatVector::Validity(result);
	auto buffer_ptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	D_ASSERT(scan_state.block_id == segment.block->BlockId());
	auto input_data = reinterpret_cast<validity_t *>(buffer_ptr);

#ifdef DEBUG
	// this method relies on all the bits we are going to write to being set to valid
	for (idx_t i = 0; i < scan_count; i++) {
		D_ASSERT(result_mask.RowIsValid(result_offset + i));
	}
#endif
#if STANDARD_VECTOR_SIZE < 128
	// fallback for tiny vector sizes
	// the bitwise ops we use below don't work if the vector size is too small
	ValidityMask source_mask(input_data);
	for (idx_t i = 0; i < scan_count; i++) {
		if (!source_mask.RowIsValid(start + i)) {
			if (result_mask.AllValid()) {
				result_mask.Initialize(MaxValue<idx_t>(STANDARD_VECTOR_SIZE, result_offset + scan_count));
			}
			result_mask.SetInvalid(result_offset + i);
		}
	}
#else
	// the code below does what the fallback code above states, but using bitwise ops:
	auto result_data = (validity_t *)result_mask.GetData();

	// set up the initial positions
	// we need to find the validity_entry to modify, together with the bit-index WITHIN the validity entry
	idx_t result_entry = result_offset / ValidityMask::BITS_PER_VALUE;
	idx_t result_idx = result_offset - result_entry * ValidityMask::BITS_PER_VALUE;

	// same for the input: find the validity_entry we are pulling from, together with the bit-index WITHIN that entry
	idx_t input_entry = start / ValidityMask::BITS_PER_VALUE;
	idx_t input_idx = start - input_entry * ValidityMask::BITS_PER_VALUE;

	// now start the bit games
	idx_t pos = 0;
	while (pos < scan_count) {
		// these are the current validity entries we are dealing with
		idx_t current_result_idx = result_entry;
		idx_t offset;
		validity_t input_mask = input_data[input_entry];

		// construct the mask to AND together with the result
		if (result_idx < input_idx) {
			// we have to shift the input RIGHT if the result_idx is smaller than the input_idx
			auto shift_amount = input_idx - result_idx;
			D_ASSERT(shift_amount > 0 && shift_amount <= ValidityMask::BITS_PER_VALUE);

			input_mask = input_mask >> shift_amount;

			// now the upper "shift_amount" bits are set to 0
			// we need them to be set to 1
			// otherwise the subsequent bitwise & will modify values outside of the range of values we want to alter
			input_mask |= ValidityUncompressed::UPPER_MASKS[shift_amount];

			// after this, we move to the next input_entry
			offset = ValidityMask::BITS_PER_VALUE - input_idx;
			input_entry++;
			input_idx = 0;
			result_idx += offset;
		} else if (result_idx > input_idx) {
			// we have to shift the input LEFT if the result_idx is bigger than the input_idx
			auto shift_amount = result_idx - input_idx;
			D_ASSERT(shift_amount > 0 && shift_amount <= ValidityMask::BITS_PER_VALUE);

			// to avoid overflows, we set the upper "shift_amount" values to 0 first
			input_mask = (input_mask & ~ValidityUncompressed::UPPER_MASKS[shift_amount]) << shift_amount;

			// now the lower "shift_amount" bits are set to 0
			// we need them to be set to 1
			// otherwise the subsequent bitwise & will modify values outside of the range of values we want to alter
			input_mask |= ValidityUncompressed::LOWER_MASKS[shift_amount];

			// after this, we move to the next result_entry
			offset = ValidityMask::BITS_PER_VALUE - result_idx;
			result_entry++;
			result_idx = 0;
			input_idx += offset;
		} else {
			// if the input_idx is equal to result_idx they are already aligned
			// we just move to the next entry for both after this
			offset = ValidityMask::BITS_PER_VALUE - result_idx;
			input_entry++;
			result_entry++;
			result_idx = input_idx = 0;
		}
		// now we need to check if we should include the ENTIRE mask
		// OR if we need to mask from the right side
		pos += offset;
		if (pos > scan_count) {
			// we need to set any bits that are past the scan_count on the right-side to 1
			// this is required so we don't influence any bits that are not part of the scan
			input_mask |= ValidityUncompressed::UPPER_MASKS[pos - scan_count];
		}
		// now finally we can merge the input mask with the result mask
		if (input_mask != ValidityMask::ValidityBuffer::MAX_ENTRY) {
			if (!result_data) {
				result_mask.Initialize(MaxValue<idx_t>(STANDARD_VECTOR_SIZE, result_offset + scan_count));
				result_data = (validity_t *)result_mask.GetData();
			}
			result_data[current_result_idx] &= input_mask;
		}
	}
#endif

#ifdef DEBUG
	// verify that we actually accomplished the bitwise ops equivalent that we wanted to do
	ValidityMask input_mask(input_data);
	for (idx_t i = 0; i < scan_count; i++) {
		D_ASSERT(result_mask.RowIsValid(result_offset + i) == input_mask.RowIsValid(start + i));
	}
#endif
}

void ValidityScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	result.Flatten(scan_count);

	auto start = segment.GetRelativeIndex(state.row_index);
	if (start % ValidityMask::BITS_PER_VALUE == 0) {
		auto &scan_state = state.scan_state->Cast<ValidityScanState>();

		// aligned scan: no need to do anything fancy
		// note: this is only an optimization which avoids having to do messy bitshifting in the common case
		// it is not required for correctness
		auto &result_mask = FlatVector::Validity(result);
		auto buffer_ptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
		D_ASSERT(scan_state.block_id == segment.block->BlockId());
		auto input_data = reinterpret_cast<validity_t *>(buffer_ptr);
		auto result_data = result_mask.GetData();
		idx_t start_offset = start / ValidityMask::BITS_PER_VALUE;
		idx_t entry_scan_count = (scan_count + ValidityMask::BITS_PER_VALUE - 1) / ValidityMask::BITS_PER_VALUE;
		for (idx_t i = 0; i < entry_scan_count; i++) {
			auto input_entry = input_data[start_offset + i];
			if (!result_data && input_entry == ValidityMask::ValidityBuffer::MAX_ENTRY) {
				continue;
			}
			if (!result_data) {
				result_mask.Initialize(MaxValue<idx_t>(STANDARD_VECTOR_SIZE, scan_count));
				result_data = result_mask.GetData();
			}
			result_data[i] = input_entry;
		}
	} else {
		// unaligned scan: fall back to scan_partial which does bitshift tricks
		ValidityScanPartial(segment, state, scan_count, result, 0);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void ValidityFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	D_ASSERT(row_id >= 0 && row_id < row_t(segment.count));
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto dataptr = handle.Ptr() + segment.GetBlockOffset();
	ValidityMask mask(reinterpret_cast<validity_t *>(dataptr));
	auto &result_mask = FlatVector::Validity(result);
	if (!mask.RowIsValidUnsafe(row_id)) {
		result_mask.SetInvalid(result_idx);
	}
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static unique_ptr<CompressionAppendState> ValidityInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	return make_uniq<CompressionAppendState>(std::move(handle));
}

unique_ptr<CompressedSegmentState> ValidityInitSegment(ColumnSegment &segment, block_id_t block_id,
                                                       optional_ptr<ColumnSegmentState> segment_state) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	if (block_id == INVALID_BLOCK) {
		auto handle = buffer_manager.Pin(segment.block);
		memset(handle.Ptr(), 0xFF, segment.SegmentSize());
	}
	return nullptr;
}

idx_t ValidityAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                     UnifiedVectorFormat &data, idx_t offset, idx_t vcount) {
	D_ASSERT(segment.GetBlockOffset() == 0);
	auto &validity_stats = stats.statistics;

	auto max_tuples = segment.SegmentSize() / ValidityMask::STANDARD_MASK_SIZE * STANDARD_VECTOR_SIZE;
	idx_t append_count = MinValue<idx_t>(vcount, max_tuples - segment.count);
	if (data.validity.AllValid()) {
		// no null values: skip append
		segment.count += append_count;
		validity_stats.SetHasNoNull();
		return append_count;
	}

	ValidityMask mask(reinterpret_cast<validity_t *>(append_state.handle.Ptr()));
	for (idx_t i = 0; i < append_count; i++) {
		auto idx = data.sel->get_index(offset + i);
		if (!data.validity.RowIsValidUnsafe(idx)) {
			mask.SetInvalidUnsafe(segment.count + i);
			validity_stats.SetHasNull();
		} else {
			validity_stats.SetHasNoNull();
		}
	}
	segment.count += append_count;
	return append_count;
}

idx_t ValidityFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	return ((segment.count + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE) * ValidityMask::STANDARD_MASK_SIZE;
}

void ValidityRevertAppend(ColumnSegment &segment, idx_t start_row) {
	idx_t start_bit = start_row - segment.start;

	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	idx_t revert_start;
	if (start_bit % 8 != 0) {
		// handle sub-bit stuff (yay)
		idx_t byte_pos = start_bit / 8;
		idx_t bit_end = (byte_pos + 1) * 8;
		ValidityMask mask(reinterpret_cast<validity_t *>(handle.Ptr()));
		for (idx_t i = start_bit; i < bit_end; i++) {
			mask.SetValid(i);
		}
		revert_start = bit_end / 8;
	} else {
		revert_start = start_bit / 8;
	}
	// for the rest, we just memset
	memset(handle.Ptr() + revert_start, 0xFF, segment.SegmentSize() - revert_start);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction ValidityUncompressed::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::BIT);
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type, ValidityInitAnalyze,
	                           ValidityAnalyze, ValidityFinalAnalyze, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           ValidityInitScan, ValidityScan, ValidityScanPartial, ValidityFetchRow,
	                           UncompressedFunctions::EmptySkip, ValidityInitSegment, ValidityInitAppend,
	                           ValidityAppend, ValidityFinalizeAppend, ValidityRevertAppend);
}

} // namespace duckdb






namespace duckdb {

unique_ptr<ColumnSegmentState> ColumnSegmentState::Deserialize(Deserializer &deserializer) {
	auto compression_type = deserializer.Get<CompressionType>();
	auto &db = deserializer.Get<DatabaseInstance &>();
	auto &type = deserializer.Get<LogicalType &>();
	auto compression_function = DBConfig::GetConfig(db).GetCompressionFunction(compression_type, type.InternalType());
	if (!compression_function || !compression_function->deserialize_state) {
		throw SerializationException("Deserializing a ColumnSegmentState but could not find deserialize method");
	}
	return compression_function->deserialize_state(deserializer);
}

} // namespace duckdb




























namespace duckdb {

DataTableInfo::DataTableInfo(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, string schema,
                             string table)
    : db(db), table_io_manager(std::move(table_io_manager_p)), cardinality(0), schema(std::move(schema)),
      table(std::move(table)) {
}

bool DataTableInfo::IsTemporary() const {
	return db.IsTemporary();
}

DataTable::DataTable(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager_p, const string &schema,
                     const string &table, vector<ColumnDefinition> column_definitions_p,
                     unique_ptr<PersistentTableData> data)
    : info(make_shared<DataTableInfo>(db, std::move(table_io_manager_p), schema, table)),
      column_definitions(std::move(column_definitions_p)), db(db), is_root(true) {
	// initialize the table with the existing data from disk, if any
	auto types = GetTypes();
	this->row_groups =
	    make_shared<RowGroupCollection>(info, TableIOManager::Get(*this).GetBlockManagerForRowData(), types, 0);
	if (data && data->row_group_count > 0) {
		this->row_groups->Initialize(*data);
	} else {
		this->row_groups->InitializeEmpty();
		D_ASSERT(row_groups->GetTotalRows() == 0);
	}
	row_groups->Verify();
}

DataTable::DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression &default_value)
    : info(parent.info), db(parent.db), is_root(true) {
	// add the column definitions from this DataTable
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	column_definitions.emplace_back(new_column.Copy());
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);

	this->row_groups = parent.row_groups->AddColumn(context, new_column, default_value);

	// also add this column to client local storage
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.AddColumn(parent, *this, new_column, default_value);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t removed_column)
    : info(parent.info), db(parent.db), is_root(true) {
	// prevent any new tuples from being added to the parent
	lock_guard<mutex> parent_lock(parent.append_lock);

	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	// first check if there are any indexes that exist that point to the removed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.column_ids) {
			if (column_id == removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on it!");
			} else if (column_id > removed_column) {
				throw CatalogException("Cannot drop this column: an index depends on a column after it!");
			}
		}
		return false;
	});

	// erase the column definitions from this DataTable
	D_ASSERT(removed_column < column_definitions.size());
	column_definitions.erase(column_definitions.begin() + removed_column);

	storage_t storage_idx = 0;
	for (idx_t i = 0; i < column_definitions.size(); i++) {
		auto &col = column_definitions[i];
		col.SetOid(i);
		if (col.Generated()) {
			continue;
		}
		col.SetStorageOid(storage_idx++);
	}

	// alter the row_groups and remove the column from each of them
	this->row_groups = parent.row_groups->RemoveColumn(removed_column);

	// scan the original table, and fill the new column with the transformed value
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.DropColumn(parent, *this, removed_column);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

// Alter column to add new constraint
DataTable::DataTable(ClientContext &context, DataTable &parent, unique_ptr<BoundConstraint> constraint)
    : info(parent.info), db(parent.db), row_groups(parent.row_groups), is_root(true) {

	lock_guard<mutex> parent_lock(parent.append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}

	// Verify the new constraint against current persistent/local data
	VerifyNewConstraint(context, parent, constraint.get());

	// Get the local data ownership from old dt
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.MoveStorage(parent, *this);
	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

DataTable::DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
                     const vector<column_t> &bound_columns, Expression &cast_expr)
    : info(parent.info), db(parent.db), is_root(true) {
	// prevent any tuples from being added to the parent
	lock_guard<mutex> lock(append_lock);
	for (auto &column_def : parent.column_definitions) {
		column_definitions.emplace_back(column_def.Copy());
	}
	// first check if there are any indexes that exist that point to the changed column
	info->indexes.Scan([&](Index &index) {
		for (auto &column_id : index.column_ids) {
			if (column_id == changed_idx) {
				throw CatalogException("Cannot change the type of this column: an index depends on it!");
			}
		}
		return false;
	});

	// change the type in this DataTable
	column_definitions[changed_idx].SetType(target_type);

	// set up the statistics for the table
	// the column that had its type changed will have the new statistics computed during conversion
	this->row_groups = parent.row_groups->AlterType(context, changed_idx, target_type, bound_columns, cast_expr);

	// scan the original table, and fill the new column with the transformed value
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.ChangeType(parent, *this, changed_idx, target_type, bound_columns, cast_expr);

	// this table replaces the previous table, hence the parent is no longer the root DataTable
	parent.is_root = false;
}

vector<LogicalType> DataTable::GetTypes() {
	vector<LogicalType> types;
	for (auto &it : column_definitions) {
		types.push_back(it.Type());
	}
	return types;
}

TableIOManager &TableIOManager::Get(DataTable &table) {
	return *table.info->table_io_manager;
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	state.Initialize(column_ids, table_filters);
	row_groups->InitializeScan(state.table_state, column_ids, table_filters);
}

void DataTable::InitializeScan(DuckTransaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
                               TableFilterSet *table_filters) {
	InitializeScan(state, column_ids, table_filters);
	auto &local_storage = LocalStorage::Get(transaction);
	local_storage.InitializeScan(*this, state.local_state, table_filters);
}

void DataTable::InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids, idx_t start_row,
                                         idx_t end_row) {
	state.Initialize(column_ids);
	row_groups->InitializeScanWithOffset(state.table_state, column_ids, start_row, end_row);
}

idx_t DataTable::MaxThreads(ClientContext &context) {
	idx_t parallel_scan_vector_count = Storage::ROW_GROUP_VECTOR_COUNT;
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		parallel_scan_vector_count = 1;
	}
	idx_t parallel_scan_tuple_count = STANDARD_VECTOR_SIZE * parallel_scan_vector_count;
	return GetTotalRows() / parallel_scan_tuple_count + 1;
}

void DataTable::InitializeParallelScan(ClientContext &context, ParallelTableScanState &state) {
	row_groups->InitializeParallelScan(state.scan_state);

	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.InitializeParallelScan(*this, state.local_state);
}

bool DataTable::NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state) {
	if (row_groups->NextParallelScan(context, state.scan_state, scan_state.table_state)) {
		return true;
	}
	scan_state.table_state.batch_index = state.scan_state.batch_index;
	auto &local_storage = LocalStorage::Get(context, db);
	if (local_storage.NextParallelScan(context, *this, state.local_state, scan_state.local_state)) {
		return true;
	} else {
		// finished all scans: no more scans remaining
		return false;
	}
}

void DataTable::Scan(DuckTransaction &transaction, DataChunk &result, TableScanState &state) {
	// scan the persistent segments
	if (state.table_state.Scan(transaction, result)) {
		D_ASSERT(result.size() > 0);
		return;
	}

	// scan the transaction-local segments
	auto &local_storage = LocalStorage::Get(transaction);
	local_storage.Scan(state.local_state, state.GetColumnIds(), result);
}

bool DataTable::CreateIndexScan(TableScanState &state, DataChunk &result, TableScanType type) {
	return state.table_state.ScanCommitted(result, type);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DataTable::Fetch(DuckTransaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
                      const Vector &row_identifiers, idx_t fetch_count, ColumnFetchState &state) {
	row_groups->Fetch(transaction, result, column_ids, row_identifiers, fetch_count, state);
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static void VerifyNotNullConstraint(TableCatalogEntry &table, Vector &vector, idx_t count, const string &col_name) {
	if (!VectorOperations::HasNull(vector, count)) {
		return;
	}

	throw ConstraintException("NOT NULL constraint failed: %s.%s", table.name, col_name);
}

// To avoid throwing an error at SELECT, instead this moves the error detection to INSERT
static void VerifyGeneratedExpressionSuccess(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
                                             Expression &expr, column_t index) {
	auto &col = table.GetColumn(LogicalIndex(index));
	D_ASSERT(col.Generated());
	ExpressionExecutor executor(context, expr);
	Vector result(col.Type());
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (InternalException &ex) {
		throw;
	} catch (std::exception &ex) {
		throw ConstraintException("Incorrect value for generated column \"%s %s AS (%s)\" : %s", col.Name(),
		                          col.Type().ToString(), col.GeneratedExpression().ToString(), ex.what());
	}
}

static void VerifyCheckConstraint(ClientContext &context, TableCatalogEntry &table, Expression &expr,
                                  DataChunk &chunk) {
	ExpressionExecutor executor(context, expr);
	Vector result(LogicalType::INTEGER);
	try {
		executor.ExecuteExpression(chunk, result);
	} catch (std::exception &ex) {
		throw ConstraintException("CHECK constraint failed: %s (Error: %s)", table.name, ex.what());
	} catch (...) { // LCOV_EXCL_START
		throw ConstraintException("CHECK constraint failed: %s (Unknown Error)", table.name);
	} // LCOV_EXCL_STOP
	UnifiedVectorFormat vdata;
	result.ToUnifiedFormat(chunk.size(), vdata);

	auto dataptr = UnifiedVectorFormat::GetData<int32_t>(vdata);
	for (idx_t i = 0; i < chunk.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx) && dataptr[idx] == 0) {
			throw ConstraintException("CHECK constraint failed: %s", table.name);
		}
	}
}

bool DataTable::IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, Index &index, ForeignKeyType fk_type) {
	if (fk_type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ? !index.IsUnique() : !index.IsForeign()) {
		return false;
	}
	if (fk_keys.size() != index.column_ids.size()) {
		return false;
	}
	for (auto &fk_key : fk_keys) {
		bool is_found = false;
		for (auto &index_key : index.column_ids) {
			if (fk_key.index == index_key) {
				is_found = true;
				break;
			}
		}
		if (!is_found) {
			return false;
		}
	}
	return true;
}

// Find the first index that is not null, and did not find a match
static idx_t FirstMissingMatch(const ManagedSelection &matches) {
	idx_t match_idx = 0;

	for (idx_t i = 0; i < matches.Size(); i++) {
		auto match = matches.IndexMapsToLocation(match_idx, i);
		match_idx += match;
		if (!match) {
			// This index is missing in the matches vector
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

idx_t LocateErrorIndex(bool is_append, const ManagedSelection &matches) {
	idx_t failed_index = DConstants::INVALID_INDEX;
	if (!is_append) {
		// We expected to find nothing, so the first error is the first match
		failed_index = matches[0];
	} else {
		// We expected to find matches for all of them, so the first missing match is the first error
		return FirstMissingMatch(matches);
	}
	return failed_index;
}

[[noreturn]] static void ThrowForeignKeyConstraintError(idx_t failed_index, bool is_append, Index &index,
                                                        DataChunk &input) {
	auto verify_type = is_append ? VerifyExistenceType::APPEND_FK : VerifyExistenceType::DELETE_FK;

	D_ASSERT(failed_index != DConstants::INVALID_INDEX);
	D_ASSERT(index.type == IndexType::ART);
	auto &art_index = index.Cast<ART>();
	auto key_name = art_index.GenerateErrorKeyName(input, failed_index);
	auto exception_msg = art_index.GenerateConstraintErrorMessage(verify_type, key_name);
	throw ConstraintException(exception_msg);
}

bool IsForeignKeyConstraintError(bool is_append, idx_t input_count, const ManagedSelection &matches) {
	if (is_append) {
		// We need to find a match for all of the values
		return matches.Count() != input_count;
	} else {
		// We should not find any matches
		return matches.Count() != 0;
	}
}

static bool IsAppend(VerifyExistenceType verify_type) {
	return verify_type == VerifyExistenceType::APPEND_FK;
}

void DataTable::VerifyForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                           DataChunk &chunk, VerifyExistenceType verify_type) {
	const vector<PhysicalIndex> *src_keys_ptr = &bfk.info.fk_keys;
	const vector<PhysicalIndex> *dst_keys_ptr = &bfk.info.pk_keys;

	bool is_append = IsAppend(verify_type);
	if (!is_append) {
		src_keys_ptr = &bfk.info.pk_keys;
		dst_keys_ptr = &bfk.info.fk_keys;
	}

	auto &table_entry_ptr =
	    Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, bfk.info.schema, bfk.info.table);
	// make the data chunk to check
	vector<LogicalType> types;
	for (auto &col : table_entry_ptr.GetColumns().Physical()) {
		types.emplace_back(col.Type());
	}
	DataChunk dst_chunk;
	dst_chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < src_keys_ptr->size(); i++) {
		dst_chunk.data[(*dst_keys_ptr)[i].index].Reference(chunk.data[(*src_keys_ptr)[i].index]);
	}
	dst_chunk.SetCardinality(chunk.size());
	auto &data_table = table_entry_ptr.GetStorage();

	idx_t count = dst_chunk.size();
	if (count <= 0) {
		return;
	}

	// Set up a way to record conflicts, rather than directly throw on them
	unordered_set<column_t> empty_column_list;
	ConflictInfo empty_conflict_info(empty_column_list, false);
	ConflictManager regular_conflicts(verify_type, count, &empty_conflict_info);
	ConflictManager transaction_conflicts(verify_type, count, &empty_conflict_info);
	regular_conflicts.SetMode(ConflictManagerMode::SCAN);
	transaction_conflicts.SetMode(ConflictManagerMode::SCAN);

	data_table.info->indexes.VerifyForeignKey(*dst_keys_ptr, dst_chunk, regular_conflicts);
	regular_conflicts.Finalize();
	auto &regular_matches = regular_conflicts.Conflicts();

	// check if we can insert the chunk into the reference table's local storage
	auto &local_storage = LocalStorage::Get(context, db);
	bool error = IsForeignKeyConstraintError(is_append, count, regular_matches);
	bool transaction_error = false;
	bool transaction_check = local_storage.Find(data_table);

	if (transaction_check) {
		auto &transact_index = local_storage.GetIndexes(data_table);
		transact_index.VerifyForeignKey(*dst_keys_ptr, dst_chunk, transaction_conflicts);
		transaction_conflicts.Finalize();
		auto &transaction_matches = transaction_conflicts.Conflicts();
		transaction_error = IsForeignKeyConstraintError(is_append, count, transaction_matches);
	}

	if (!transaction_error && !error) {
		// No error occurred;
		return;
	}

	// Some error occurred, and we likely want to throw
	optional_ptr<Index> index;
	optional_ptr<Index> transaction_index;

	auto fk_type = is_append ? ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE : ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
	// check whether or not the chunk can be inserted or deleted into the referenced table' storage
	index = data_table.info->indexes.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	if (transaction_check) {
		auto &transact_index = local_storage.GetIndexes(data_table);
		// check whether or not the chunk can be inserted or deleted into the referenced table' storage
		transaction_index = transact_index.FindForeignKeyIndex(*dst_keys_ptr, fk_type);
	}

	if (!transaction_check) {
		// Only local state is checked, throw the error
		D_ASSERT(error);
		auto failed_index = LocateErrorIndex(is_append, regular_matches);
		D_ASSERT(failed_index != DConstants::INVALID_INDEX);
		ThrowForeignKeyConstraintError(failed_index, is_append, *index, dst_chunk);
	}
	if (transaction_error && error && is_append) {
		// When we want to do an append, we only throw if the foreign key does not exist in both transaction and local
		// storage
		auto &transaction_matches = transaction_conflicts.Conflicts();
		idx_t failed_index = DConstants::INVALID_INDEX;
		idx_t regular_idx = 0;
		idx_t transaction_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			bool in_regular = regular_matches.IndexMapsToLocation(regular_idx, i);
			regular_idx += in_regular;
			bool in_transaction = transaction_matches.IndexMapsToLocation(transaction_idx, i);
			transaction_idx += in_transaction;

			if (!in_regular && !in_transaction) {
				// We need to find a match for all of the input values
				// The failed index is i, it does not show up in either regular or transaction storage
				failed_index = i;
				break;
			}
		}
		if (failed_index == DConstants::INVALID_INDEX) {
			// We don't throw, every value was present in either regular or transaction storage
			return;
		}
		ThrowForeignKeyConstraintError(failed_index, true, *index, dst_chunk);
	}
	if (!is_append && transaction_check) {
		auto &transaction_matches = transaction_conflicts.Conflicts();
		if (error) {
			auto failed_index = LocateErrorIndex(false, regular_matches);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, *index, dst_chunk);
		} else {
			D_ASSERT(transaction_error);
			D_ASSERT(transaction_matches.Count() != DConstants::INVALID_INDEX);
			auto failed_index = LocateErrorIndex(false, transaction_matches);
			D_ASSERT(failed_index != DConstants::INVALID_INDEX);
			ThrowForeignKeyConstraintError(failed_index, false, *transaction_index, dst_chunk);
		}
	}
}

void DataTable::VerifyAppendForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                                 DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, VerifyExistenceType::APPEND_FK);
}

void DataTable::VerifyDeleteForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
                                                 DataChunk &chunk) {
	VerifyForeignKeyConstraint(bfk, context, chunk, VerifyExistenceType::DELETE_FK);
}

void DataTable::VerifyNewConstraint(ClientContext &context, DataTable &parent, const BoundConstraint *constraint) {
	if (constraint->type != ConstraintType::NOT_NULL) {
		throw NotImplementedException("FIXME: ALTER COLUMN with such constraint is not supported yet");
	}

	parent.row_groups->VerifyNewConstraint(parent, *constraint);
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.VerifyNewConstraint(parent, *constraint);
}

bool HasUniqueIndexes(TableIndexList &list) {
	bool has_unique_index = false;
	list.Scan([&](Index &index) {
		if (index.IsUnique()) {
			return has_unique_index = true;
			return true;
		}
		return false;
	});
	return has_unique_index;
}

void DataTable::VerifyUniqueIndexes(TableIndexList &indexes, ClientContext &context, DataChunk &chunk,
                                    ConflictManager *conflict_manager) {
	//! check whether or not the chunk can be inserted into the indexes
	if (!conflict_manager) {
		// Only need to verify that no unique constraints are violated
		indexes.Scan([&](Index &index) {
			if (!index.IsUnique()) {
				return false;
			}
			index.VerifyAppend(chunk);
			return false;
		});
		return;
	}

	D_ASSERT(conflict_manager);
	// The conflict manager is only provided when a ON CONFLICT clause was provided to the INSERT statement

	idx_t matching_indexes = 0;
	auto &conflict_info = conflict_manager->GetConflictInfo();
	// First we figure out how many indexes match our conflict target
	// So we can optimize accordingly
	indexes.Scan([&](Index &index) {
		matching_indexes += conflict_info.ConflictTargetMatches(index);
		return false;
	});
	conflict_manager->SetMode(ConflictManagerMode::SCAN);
	conflict_manager->SetIndexCount(matching_indexes);
	// First we verify only the indexes that match our conflict target
	unordered_set<Index *> checked_indexes;
	indexes.Scan([&](Index &index) {
		if (!index.IsUnique()) {
			return false;
		}
		if (conflict_info.ConflictTargetMatches(index)) {
			index.VerifyAppend(chunk, *conflict_manager);
			checked_indexes.insert(&index);
		}
		return false;
	});

	conflict_manager->SetMode(ConflictManagerMode::THROW);
	// Then we scan the other indexes, throwing if they cause conflicts on tuples that were not found during
	// the scan
	indexes.Scan([&](Index &index) {
		if (!index.IsUnique()) {
			return false;
		}
		if (checked_indexes.count(&index)) {
			// Already checked this constraint
			return false;
		}
		index.VerifyAppend(chunk, *conflict_manager);
		return false;
	});
}

void DataTable::VerifyAppendConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                                        ConflictManager *conflict_manager) {
	if (table.HasGeneratedColumns()) {
		// Verify that the generated columns expression work with the inserted values
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder generated_check_binder(*binder, context, table.name, table.GetColumns(), bound_columns);
		for (auto &col : table.GetColumns().Logical()) {
			if (!col.Generated()) {
				continue;
			}
			D_ASSERT(col.Type().id() != LogicalTypeId::ANY);
			generated_check_binder.target_type = col.Type();
			auto to_be_bound_expression = col.GeneratedExpression().Copy();
			auto bound_expression = generated_check_binder.Bind(to_be_bound_expression);
			VerifyGeneratedExpressionSuccess(context, table, chunk, *bound_expression, col.Oid());
		}
	}

	if (HasUniqueIndexes(info->indexes)) {
		VerifyUniqueIndexes(info->indexes, context, chunk, conflict_manager);
	}

	auto &constraints = table.GetConstraints();
	auto &bound_constraints = table.GetBoundConstraints();
	for (idx_t i = 0; i < bound_constraints.size(); i++) {
		auto &base_constraint = constraints[i];
		auto &constraint = bound_constraints[i];
		switch (base_constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			auto &not_null = *reinterpret_cast<NotNullConstraint *>(base_constraint.get());
			auto &col = table.GetColumns().GetColumn(LogicalIndex(not_null.index));
			VerifyNotNullConstraint(table, chunk.data[bound_not_null.index.index], chunk.size(), col.Name());
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());
			VerifyCheckConstraint(context, table, *check.expression, chunk);
			break;
		}
		case ConstraintType::UNIQUE: {
			// These were handled earlier on
			break;
		}
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = *reinterpret_cast<BoundForeignKeyConstraint *>(constraint.get());
			if (bfk.info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				VerifyAppendForeignKeyConstraint(bfk, context, chunk);
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

void DataTable::InitializeLocalAppend(LocalAppendState &state, ClientContext &context) {
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.InitializeAppend(state, *this);
}

void DataTable::LocalAppend(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
                            bool unsafe) {
	if (chunk.size() == 0) {
		return;
	}
	D_ASSERT(chunk.ColumnCount() == table.GetColumns().PhysicalColumnCount());
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}

	chunk.Verify();

	// verify any constraints on the new chunk
	if (!unsafe) {
		VerifyAppendConstraints(table, context, chunk);
	}

	// append to the transaction local data
	LocalStorage::Append(state, chunk);
}

void DataTable::FinalizeLocalAppend(LocalAppendState &state) {
	LocalStorage::FinalizeAppend(state);
}

OptimisticDataWriter &DataTable::CreateOptimisticWriter(ClientContext &context) {
	auto &local_storage = LocalStorage::Get(context, db);
	return local_storage.CreateOptimisticWriter(*this);
}

void DataTable::FinalizeOptimisticWriter(ClientContext &context, OptimisticDataWriter &writer) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.FinalizeOptimisticWriter(*this, writer);
}

void DataTable::LocalMerge(ClientContext &context, RowGroupCollection &collection) {
	auto &local_storage = LocalStorage::Get(context, db);
	local_storage.LocalMerge(*this, collection);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, context);
	storage.LocalAppend(append_state, table, context, chunk);
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::LocalAppend(TableCatalogEntry &table, ClientContext &context, ColumnDataCollection &collection) {
	LocalAppendState append_state;
	auto &storage = table.GetStorage();
	storage.InitializeLocalAppend(append_state, context);
	for (auto &chunk : collection.Chunks()) {
		storage.LocalAppend(append_state, table, context, chunk);
	}
	storage.FinalizeLocalAppend(append_state);
}

void DataTable::AppendLock(TableAppendState &state) {
	state.append_lock = unique_lock<mutex>(append_lock);
	if (!is_root) {
		throw TransactionException("Transaction conflict: adding entries to a table that has been altered!");
	}
	state.row_start = row_groups->GetTotalRows();
	state.current_row = state.row_start;
}

void DataTable::InitializeAppend(DuckTransaction &transaction, TableAppendState &state, idx_t append_count) {
	// obtain the append lock for this table
	if (!state.append_lock) {
		throw InternalException("DataTable::AppendLock should be called before DataTable::InitializeAppend");
	}
	row_groups->InitializeAppend(transaction, state, append_count);
}

void DataTable::Append(DataChunk &chunk, TableAppendState &state) {
	D_ASSERT(is_root);
	row_groups->Append(chunk, state);
}

void DataTable::ScanTableSegment(idx_t row_start, idx_t count, const std::function<void(DataChunk &chunk)> &function) {
	if (count == 0) {
		return;
	}
	idx_t end = row_start + count;

	vector<column_t> column_ids;
	vector<LogicalType> types;
	for (idx_t i = 0; i < this->column_definitions.size(); i++) {
		auto &col = this->column_definitions[i];
		column_ids.push_back(i);
		types.push_back(col.Type());
	}
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(db), types);

	CreateIndexScanState state;

	InitializeScanWithOffset(state, column_ids, row_start, row_start + count);
	auto row_start_aligned = state.table_state.row_group->start + state.table_state.vector_index * STANDARD_VECTOR_SIZE;

	idx_t current_row = row_start_aligned;
	while (current_row < end) {
		state.table_state.ScanCommitted(chunk, TableScanType::TABLE_SCAN_COMMITTED_ROWS);
		if (chunk.size() == 0) {
			break;
		}
		idx_t end_row = current_row + chunk.size();
		// start of chunk is current_row
		// end of chunk is end_row
		// figure out if we need to write the entire chunk or just part of it
		idx_t chunk_start = MaxValue<idx_t>(current_row, row_start);
		idx_t chunk_end = MinValue<idx_t>(end_row, end);
		D_ASSERT(chunk_start < chunk_end);
		idx_t chunk_count = chunk_end - chunk_start;
		if (chunk_count != chunk.size()) {
			D_ASSERT(chunk_count <= chunk.size());
			// need to slice the chunk before insert
			idx_t start_in_chunk;
			if (current_row >= row_start) {
				start_in_chunk = 0;
			} else {
				start_in_chunk = row_start - current_row;
			}
			SelectionVector sel(start_in_chunk, chunk_count);
			chunk.Slice(sel, chunk_count);
			chunk.Verify();
		}
		function(chunk);
		chunk.Reset();
		current_row = end_row;
	}
}

void DataTable::MergeStorage(RowGroupCollection &data, TableIndexList &indexes) {
	row_groups->MergeStorage(data);
	row_groups->Verify();
}

void DataTable::WriteToLog(WriteAheadLog &log, idx_t row_start, idx_t count) {
	if (log.skip_writing) {
		return;
	}
	log.WriteSetTable(info->schema, info->table);
	ScanTableSegment(row_start, count, [&](DataChunk &chunk) { log.WriteInsert(chunk); });
}

void DataTable::CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count) {
	lock_guard<mutex> lock(append_lock);
	row_groups->CommitAppend(commit_id, row_start, count);
	info->cardinality += count;
}

void DataTable::RevertAppendInternal(idx_t start_row) {
	// adjust the cardinality
	info->cardinality = start_row;
	D_ASSERT(is_root);
	// revert appends made to row_groups
	row_groups->RevertAppendInternal(start_row);
}

void DataTable::RevertAppend(idx_t start_row, idx_t count) {
	lock_guard<mutex> lock(append_lock);

	// revert any appends to indexes
	if (!info->indexes.Empty()) {
		idx_t current_row_base = start_row;
		row_t row_data[STANDARD_VECTOR_SIZE];
		Vector row_identifiers(LogicalType::ROW_TYPE, data_ptr_cast(row_data));
		idx_t scan_count = MinValue<idx_t>(count, row_groups->GetTotalRows() - start_row);
		ScanTableSegment(start_row, scan_count, [&](DataChunk &chunk) {
			for (idx_t i = 0; i < chunk.size(); i++) {
				row_data[i] = current_row_base + i;
			}
			info->indexes.Scan([&](Index &index) {
				index.Delete(chunk, row_identifiers);
				return false;
			});
			current_row_base += chunk.size();
		});
	}

	// we need to vacuum the indexes to remove any buffers that are now empty
	// due to reverting the appends
	info->indexes.Scan([&](Index &index) {
		index.Vacuum();
		return false;
	});

	// revert the data table append
	RevertAppendInternal(start_row);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
PreservedError DataTable::AppendToIndexes(TableIndexList &indexes, DataChunk &chunk, row_t row_start) {
	PreservedError error;
	if (indexes.Empty()) {
		return error;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	vector<Index *> already_appended;
	bool append_failed = false;
	// now append the entries to the indices
	indexes.Scan([&](Index &index) {
		try {
			error = index.Append(chunk, row_identifiers);
		} catch (Exception &ex) {
			error = PreservedError(ex);
		} catch (std::exception &ex) {
			error = PreservedError(ex);
		}
		if (error) {
			append_failed = true;
			return true;
		}
		already_appended.push_back(&index);
		return false;
	});

	if (append_failed) {
		// constraint violation!
		// remove any appended entries from previous indexes (if any)
		for (auto *index : already_appended) {
			index->Delete(chunk, row_identifiers);
		}
	}
	return error;
}

PreservedError DataTable::AppendToIndexes(DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	return AppendToIndexes(info->indexes, chunk, row_start);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start) {
	D_ASSERT(is_root);
	if (info->indexes.Empty()) {
		return;
	}
	// first generate the vector of row identifiers
	Vector row_identifiers(LogicalType::ROW_TYPE);
	VectorOperations::GenerateSequence(row_identifiers, chunk.size(), row_start, 1);

	// now remove the entries from the indices
	RemoveFromIndexes(state, chunk, row_identifiers);
}

void DataTable::RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, Vector &row_identifiers) {
	D_ASSERT(is_root);
	info->indexes.Scan([&](Index &index) {
		index.Delete(chunk, row_identifiers);
		return false;
	});
}

void DataTable::RemoveFromIndexes(Vector &row_identifiers, idx_t count) {
	D_ASSERT(is_root);
	row_groups->RemoveFromIndexes(info->indexes, row_identifiers, count);
}

//===--------------------------------------------------------------------===//
// Delete
//===--------------------------------------------------------------------===//
static bool TableHasDeleteConstraints(TableCatalogEntry &table) {
	auto &bound_constraints = table.GetBoundConstraints();
	for (auto &constraint : bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = *reinterpret_cast<BoundForeignKeyConstraint *>(constraint.get());
			if (bfk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				return true;
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
	return false;
}

void DataTable::VerifyDeleteConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk) {
	auto &bound_constraints = table.GetBoundConstraints();
	for (auto &constraint : bound_constraints) {
		switch (constraint->type) {
		case ConstraintType::NOT_NULL:
		case ConstraintType::CHECK:
		case ConstraintType::UNIQUE:
			break;
		case ConstraintType::FOREIGN_KEY: {
			auto &bfk = *reinterpret_cast<BoundForeignKeyConstraint *>(constraint.get());
			if (bfk.info.type == ForeignKeyType::FK_TYPE_PRIMARY_KEY_TABLE ||
			    bfk.info.type == ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
				VerifyDeleteForeignKeyConstraint(bfk, context, chunk);
			}
			break;
		}
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
}

idx_t DataTable::Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_identifiers, idx_t count) {
	D_ASSERT(row_identifiers.GetType().InternalType() == ROW_TYPE);
	if (count == 0) {
		return 0;
	}

	auto &transaction = DuckTransaction::Get(context, db);
	auto &local_storage = LocalStorage::Get(transaction);
	bool has_delete_constraints = TableHasDeleteConstraints(table);

	row_identifiers.Flatten(count);
	auto ids = FlatVector::GetData<row_t>(row_identifiers);

	DataChunk verify_chunk;
	vector<column_t> col_ids;
	vector<LogicalType> types;
	ColumnFetchState fetch_state;
	if (has_delete_constraints) {
		// initialize the chunk if there are any constraints to verify
		for (idx_t i = 0; i < column_definitions.size(); i++) {
			col_ids.push_back(column_definitions[i].StorageOid());
			types.emplace_back(column_definitions[i].Type());
		}
		verify_chunk.Initialize(Allocator::Get(context), types);
	}
	idx_t pos = 0;
	idx_t delete_count = 0;
	while (pos < count) {
		idx_t start = pos;
		bool is_transaction_delete = ids[pos] >= MAX_ROW_ID;
		// figure out which batch of rows to delete now
		for (pos++; pos < count; pos++) {
			bool row_is_transaction_delete = ids[pos] >= MAX_ROW_ID;
			if (row_is_transaction_delete != is_transaction_delete) {
				break;
			}
		}
		idx_t current_offset = start;
		idx_t current_count = pos - start;

		Vector offset_ids(row_identifiers, current_offset, pos);
		if (is_transaction_delete) {
			// transaction-local delete
			if (has_delete_constraints) {
				// perform the constraint verification
				local_storage.FetchChunk(*this, offset_ids, current_count, col_ids, verify_chunk, fetch_state);
				VerifyDeleteConstraints(table, context, verify_chunk);
			}
			delete_count += local_storage.Delete(*this, offset_ids, current_count);
		} else {
			// regular table delete
			if (has_delete_constraints) {
				// perform the constraint verification
				Fetch(transaction, verify_chunk, col_ids, offset_ids, current_count, fetch_state);
				VerifyDeleteConstraints(table, context, verify_chunk);
			}
			delete_count += row_groups->Delete(transaction, *this, ids + current_offset, current_count);
		}
	}
	return delete_count;
}

//===--------------------------------------------------------------------===//
// Update
//===--------------------------------------------------------------------===//
static void CreateMockChunk(vector<LogicalType> &types, const vector<PhysicalIndex> &column_ids, DataChunk &chunk,
                            DataChunk &mock_chunk) {
	// construct a mock DataChunk
	mock_chunk.InitializeEmpty(types);
	for (column_t i = 0; i < column_ids.size(); i++) {
		mock_chunk.data[column_ids[i].index].Reference(chunk.data[i]);
	}
	mock_chunk.SetCardinality(chunk.size());
}

static bool CreateMockChunk(TableCatalogEntry &table, const vector<PhysicalIndex> &column_ids,
                            physical_index_set_t &desired_column_ids, DataChunk &chunk, DataChunk &mock_chunk) {
	idx_t found_columns = 0;
	// check whether the desired columns are present in the UPDATE clause
	for (column_t i = 0; i < column_ids.size(); i++) {
		if (desired_column_ids.find(column_ids[i]) != desired_column_ids.end()) {
			found_columns++;
		}
	}
	if (found_columns == 0) {
		// no columns were found: no need to check the constraint again
		return false;
	}
	if (found_columns != desired_column_ids.size()) {
		// not all columns in UPDATE clause are present!
		// this should not be triggered at all as the binder should add these columns
		throw InternalException("Not all columns required for the CHECK constraint are present in the UPDATED chunk!");
	}
	// construct a mock DataChunk
	auto types = table.GetTypes();
	CreateMockChunk(types, column_ids, chunk, mock_chunk);
	return true;
}

void DataTable::VerifyUpdateConstraints(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
                                        const vector<PhysicalIndex> &column_ids) {
	auto &constraints = table.GetConstraints();
	auto &bound_constraints = table.GetBoundConstraints();
	for (idx_t constr_idx = 0; constr_idx < bound_constraints.size(); constr_idx++) {
		auto &base_constraint = constraints[constr_idx];
		auto &constraint = bound_constraints[constr_idx];
		switch (constraint->type) {
		case ConstraintType::NOT_NULL: {
			auto &bound_not_null = *reinterpret_cast<BoundNotNullConstraint *>(constraint.get());
			auto &not_null = *reinterpret_cast<NotNullConstraint *>(base_constraint.get());
			// check if the constraint is in the list of column_ids
			for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
				if (column_ids[col_idx] == bound_not_null.index) {
					// found the column id: check the data in
					auto &col = table.GetColumn(LogicalIndex(not_null.index));
					VerifyNotNullConstraint(table, chunk.data[col_idx], chunk.size(), col.Name());
					break;
				}
			}
			break;
		}
		case ConstraintType::CHECK: {
			auto &check = *reinterpret_cast<BoundCheckConstraint *>(constraint.get());

			DataChunk mock_chunk;
			if (CreateMockChunk(table, column_ids, check.bound_columns, chunk, mock_chunk)) {
				VerifyCheckConstraint(context, table, *check.expression, mock_chunk);
			}
			break;
		}
		case ConstraintType::UNIQUE:
		case ConstraintType::FOREIGN_KEY:
			break;
		default:
			throw NotImplementedException("Constraint type not implemented!");
		}
	}
	// update should not be called for indexed columns!
	// instead update should have been rewritten to delete + update on higher layer
#ifdef DEBUG
	info->indexes.Scan([&](Index &index) {
		D_ASSERT(!index.IndexIsUpdated(column_ids));
		return false;
	});

#endif
}

void DataTable::Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
                       const vector<PhysicalIndex> &column_ids, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(column_ids.size() == updates.ColumnCount());
	updates.Verify();

	auto count = updates.size();
	if (count == 0) {
		return;
	}

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot update a table that has been altered!");
	}

	// first verify that no constraints are violated
	VerifyUpdateConstraints(context, table, updates, column_ids);

	// now perform the actual update
	Vector max_row_id_vec(Value::BIGINT(MAX_ROW_ID));
	Vector row_ids_slice(LogicalType::BIGINT);
	DataChunk updates_slice;
	updates_slice.InitializeEmpty(updates.GetTypes());

	SelectionVector sel_local_update(count), sel_global_update(count);
	auto n_local_update = VectorOperations::GreaterThanEquals(row_ids, max_row_id_vec, nullptr, count,
	                                                          &sel_local_update, &sel_global_update);
	auto n_global_update = count - n_local_update;

	// row id > MAX_ROW_ID? transaction-local storage
	if (n_local_update > 0) {
		updates_slice.Slice(updates, sel_local_update, n_local_update);
		updates_slice.Flatten();
		row_ids_slice.Slice(row_ids, sel_local_update, n_local_update);
		row_ids_slice.Flatten(n_local_update);

		LocalStorage::Get(context, db).Update(*this, row_ids_slice, column_ids, updates_slice);
	}

	// otherwise global storage
	if (n_global_update > 0) {
		updates_slice.Slice(updates, sel_global_update, n_global_update);
		updates_slice.Flatten();
		row_ids_slice.Slice(row_ids, sel_global_update, n_global_update);
		row_ids_slice.Flatten(n_global_update);

		row_groups->Update(DuckTransaction::Get(context, db), FlatVector::GetData<row_t>(row_ids_slice), column_ids,
		                   updates_slice);
	}
}

void DataTable::UpdateColumn(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
                             const vector<column_t> &column_path, DataChunk &updates) {
	D_ASSERT(row_ids.GetType().InternalType() == ROW_TYPE);
	D_ASSERT(updates.ColumnCount() == 1);
	updates.Verify();
	if (updates.size() == 0) {
		return;
	}

	if (!is_root) {
		throw TransactionException("Transaction conflict: cannot update a table that has been altered!");
	}

	// now perform the actual update
	auto &transaction = DuckTransaction::Get(context, db);

	updates.Flatten();
	row_ids.Flatten(updates.size());
	row_groups->UpdateColumn(transaction, row_ids, column_path, updates);
}

//===--------------------------------------------------------------------===//
// Index Scan
//===--------------------------------------------------------------------===//
void DataTable::InitializeWALCreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids) {
	// we grab the append lock to make sure nothing is appended until AFTER we finish the index scan
	state.append_lock = std::unique_lock<mutex>(append_lock);
	InitializeScan(state, column_ids);
}

void DataTable::WALAddIndex(ClientContext &context, unique_ptr<Index> index,
                            const vector<unique_ptr<Expression>> &expressions) {

	// if the data table is empty
	if (row_groups->IsEmpty()) {
		info->indexes.AddIndex(std::move(index));
		return;
	}

	auto &allocator = Allocator::Get(db);

	// intermediate holds scanned chunks of the underlying data to create the index
	DataChunk intermediate;
	vector<LogicalType> intermediate_types;
	vector<column_t> column_ids;
	for (auto &it : column_definitions) {
		intermediate_types.push_back(it.Type());
		column_ids.push_back(it.Oid());
	}
	column_ids.push_back(COLUMN_IDENTIFIER_ROW_ID);
	intermediate_types.emplace_back(LogicalType::ROW_TYPE);

	intermediate.Initialize(allocator, intermediate_types);

	// holds the result of executing the index expression on the intermediate chunks
	DataChunk result;
	result.Initialize(allocator, index->logical_types);

	// initialize an index scan
	CreateIndexScanState state;
	InitializeWALCreateIndexScan(state, column_ids);

	if (!is_root) {
		throw InternalException("Error during WAL replay. Cannot add an index to a table that has been altered.");
	}

	// now start incrementally building the index
	{
		IndexLock lock;
		index->InitializeLock(lock);

		while (true) {
			intermediate.Reset();
			result.Reset();
			// scan a new chunk from the table to index
			CreateIndexScan(state, intermediate, TableScanType::TABLE_SCAN_COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED);
			if (intermediate.size() == 0) {
				// finished scanning for index creation
				// release all locks
				break;
			}
			// resolve the expressions for this chunk
			index->ExecuteExpressions(intermediate, result);

			// insert into the index
			auto error = index->Insert(lock, result, intermediate.data[intermediate.ColumnCount() - 1]);
			if (error) {
				throw InternalException("Error during WAL replay: %s", error.Message());
			}
		}
	}

	info->indexes.AddIndex(std::move(index));
}

//===--------------------------------------------------------------------===//
// Statistics
//===--------------------------------------------------------------------===//
unique_ptr<BaseStatistics> DataTable::GetStatistics(ClientContext &context, column_t column_id) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		return nullptr;
	}
	return row_groups->CopyStats(column_id);
}

void DataTable::SetDistinct(column_t column_id, unique_ptr<DistinctStatistics> distinct_stats) {
	D_ASSERT(column_id != COLUMN_IDENTIFIER_ROW_ID);
	row_groups->SetDistinct(column_id, std::move(distinct_stats));
}

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
void DataTable::Checkpoint(TableDataWriter &writer, Serializer &metadata_serializer) {
	// checkpoint each individual row group
	// FIXME: we might want to combine adjacent row groups in case they have had deletions...
	TableStatistics global_stats;
	row_groups->CopyStats(global_stats);

	row_groups->Checkpoint(writer, global_stats);

	// The rowgroup payload data has been written. Now write:
	//   column stats
	//   row-group pointers
	//   table pointer
	//   index data
	writer.FinalizeTable(std::move(global_stats), info.get(), metadata_serializer);
}

void DataTable::CommitDropColumn(idx_t index) {
	row_groups->CommitDropColumn(index);
}

idx_t DataTable::GetTotalRows() {
	return row_groups->GetTotalRows();
}

void DataTable::CommitDropTable() {
	// commit a drop of this table: mark all blocks as modified so they can be reclaimed later on
	row_groups->CommitDropTable();
}

//===--------------------------------------------------------------------===//
// GetColumnSegmentInfo
//===--------------------------------------------------------------------===//
vector<ColumnSegmentInfo> DataTable::GetColumnSegmentInfo() {
	return row_groups->GetColumnSegmentInfo();
}

} // namespace duckdb

#endif
