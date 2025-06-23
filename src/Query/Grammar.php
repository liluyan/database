<?php

namespace GodJarvis\Database\Query;

class Grammar
{
    protected $tablePrefix = '';

    protected $operators = ['sounds like'];

    protected $bitwiseOperators = [];

    protected $selectComponents = [
        'aggregate',
        'columns',
        'from',
        'joins',
        'wheres',
        'groups',
        'havings',
        'orders',
        'limit',
        'offset',
        'lock',
    ];

    protected function whereNull(Builder $query, $where)
    {
        if ($this->isJsonSelector($where['column'])) {
            [$field, $path] = $this->wrapJsonFieldAndPath($where['column']);

            return '(json_extract(' . $field . $path . ') is null OR json_type(json_extract(' . $field . $path . ')) = \'NULL\')';
        }

        return $this->wrap($where['column']) . ' is null';
    }

    protected function whereNotNull(Builder $query, $where)
    {
        if ($this->isJsonSelector($where['column'])) {
            [$field, $path] = $this->wrapJsonFieldAndPath($where['column']);

            return '(json_extract(' . $field . $path . ') is not null AND json_type(json_extract(' . $field . $path . ')) != \'NULL\')';
        }

        return $this->wrap($where['column']) . ' is not null';
    }

    public function whereFullText(Builder $query, $where)
    {
        $columns = $this->columnize($where['columns']);

        $value = $this->parameter($where['value']);

        $mode = ($where['options']['mode'] ?? []) === 'boolean'
            ? ' in boolean mode'
            : ' in natural language mode';

        $expanded = ($where['options']['expanded'] ?? []) && ($where['options']['mode'] ?? []) !== 'boolean'
            ? ' with query expansion'
            : '';

        return "match ({$columns}) against (" . $value . "{$mode}{$expanded})";
    }

    protected function compileJsonContains($column, $value)
    {
        [$field, $path] = $this->wrapJsonFieldAndPath($column);

        return 'json_contains(' . $field . ', ' . $value . $path . ')';
    }

    protected function compileJsonLength($column, $operator, $value)
    {
        [$field, $path] = $this->wrapJsonFieldAndPath($column);

        return 'json_length(' . $field . $path . ') ' . $operator . ' ' . $value;
    }

    public function compileRandom($seed)
    {
        return 'RAND(' . $seed . ')';
    }

    protected function compileLock(Builder $query, $value)
    {
        if (!is_string($value)) {
            return $value ? 'for update' : 'lock in share mode';
        }

        return $value;
    }

    public function compileInsert(Builder $query, array $values)
    {
        if (empty($values)) {
            $values = [[]];
        }
        $table = $this->wrapTable($query->from);

        if (empty($values)) {
            return "insert into {$table} default values";
        }

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        $columns = $this->columnize(array_keys(reset($values)));
        $keys = array_keys($values);
        $callback = function ($record) {
            return '(' . $this->parameterize($record) . ')';
        };
        $items = array_map($callback, $values, $keys);
        $parameters = implode(', ', array_combine($keys, $items));

        return "insert into $table ($columns) values $parameters";
    }

    public function compileInsertOrIgnore(Builder $query, array $values)
    {
        $search = 'insert';
        $replace = 'insert ignore';
        $subject = $this->compileInsert($query, $values);
        $position = strpos($subject, $search);
        if ($position !== false) {
            return substr_replace($subject, $replace, $position, strlen($search));
        }
        return $subject;
    }

    public function compileDelete(Builder $query)
    {
        $table = $this->wrapTable($query->from);

        $where = $this->compileWheres($query);

        return trim(
            isset($query->joins)
                ? $this->compileDeleteWithJoins($query, $table, $where)
                : $this->compileDeleteWithoutJoins($query, $table, $where)
        );
    }

    protected function compileDeleteWithJoins(Builder $query, $table, $where)
    {
        $explodeTable = explode(' as ', $table);
        $alias = end($explodeTable);

        $joins = $this->compileJoins($query, $query->joins);

        return "delete {$alias} from {$table} {$joins} {$where}";
    }

    protected function compileDeleteWithoutJoins(Builder $query, $table, $where)
    {
        $sql = "delete from {$table} {$where}";

        if (!empty($query->orders)) {
            $sql .= ' ' . $this->compileOrders($query, $query->orders);
        }

        if (isset($query->limit)) {
            $sql .= ' ' . $this->compileLimit($query, $query->limit);
        }

        return $sql;
    }

    protected function wrapValue($value)
    {
        return $value === '*' ? $value : '`' . str_replace('`', '``', $value) . '`';
    }

    public function compileSelect(Builder $query)
    {
        if (($query->unions || $query->havings) && $query->aggregate) {
            return $this->compileUnionAggregate($query);
        }

        $original = $query->columns;

        if (is_null($query->columns)) {
            $query->columns = ['*'];
        }

        $sql = trim(
            $this->concatenate(
                $this->compileComponents($query)
            )
        );

        if ($query->unions) {
            $sql = $this->wrapUnion($sql) . ' ' . $this->compileUnions($query);
        }

        $query->columns = $original;

        return $sql;
    }

    protected function compileComponents(Builder $query)
    {
        $sql = [];

        foreach ($this->selectComponents as $component) {
            if (isset($query->$component)) {
                $method = 'compile' . ucfirst($component);

                $sql[$component] = $this->$method($query, $query->$component);
            }
        }

        return $sql;
    }

    protected function compileAggregate(Builder $query, $aggregate)
    {
        $column = $this->columnize($aggregate['columns']);

        if (is_array($query->distinct)) {
            $column = 'distinct ' . $this->columnize($query->distinct);
        } elseif ($query->distinct && $column !== '*') {
            $column = 'distinct ' . $column;
        }

        return 'select ' . $aggregate['function'] . '(' . $column . ') as ' . $aggregate['asAlias'];
    }

    protected function compileColumns(Builder $query, $columns)
    {
        if (!is_null($query->aggregate)) {
            return '';
        }

        if ($query->distinct) {
            $select = 'select distinct ';
        } else {
            $select = 'select ';
        }

        return $select . $this->columnize($columns);
    }

    protected function compileFrom(Builder $query, $table)
    {
        return 'from ' . $this->wrapTable($table);
    }

    protected function compileJoins(Builder $query, $joins)
    {
        $joins = (array)$joins;
        $keys = array_keys($joins);
        $callback = function ($join) use ($query) {
            $table = $this->wrapTable($join->table);

            $nestedJoins = is_null($join->joins) ? '' : ' ' . $this->compileJoins($query, $join->joins);

            $tableAndNestedJoins = is_null($join->joins) ? $table : '(' . $table . $nestedJoins . ')';

            return trim("{$join->type} join {$tableAndNestedJoins} {$this->compileWheres($join)}");
        };
        $items = array_map($callback, $joins, $keys);
        return implode(' ', array_combine($keys, $items));
    }

    public function compileWheres(Builder $query)
    {
        if (is_null($query->wheres)) {
            return '';
        }

        if (count($sql = $this->compileWheresToArray($query)) > 0) {
            return $this->concatenateWhereClauses($query, $sql);
        }

        return '';
    }

    protected function compileWheresToArray($query)
    {
        $wheres = (array)$query->wheres;
        $keys = array_keys($wheres);
        $callback = function ($where) use ($query) {
            return $where['boolean'] . ' ' . $this->{"where{$where['type']}"}($query, $where);
        };
        $items = array_map($callback, $wheres, $keys);
        return array_combine($keys, $items);
    }

    protected function concatenateWhereClauses($query, $sql)
    {
        $conjunction = $query instanceof JoinClause ? 'on' : 'where';

        return $conjunction . ' ' . $this->removeLeadingBoolean(implode(' ', $sql));
    }

    protected function whereRaw(Builder $query, $where)
    {
        return $where['sql'];
    }

    protected function whereBasic(Builder $query, $where)
    {
        $value = $this->parameter($where['value']);

        $operator = str_replace('?', '??', $where['operator']);

        return $this->wrap($where['column']) . ' ' . $operator . ' ' . $value;
    }

    protected function whereBitwise(Builder $query, $where)
    {
        return $this->whereBasic($query, $where);
    }

    protected function whereIn(Builder $query, $where)
    {
        if (!empty($where['values'])) {
            return $this->wrap($where['column']) . ' in (' . $this->parameterize($where['values']) . ')';
        }

        return '0 = 1';
    }

    protected function whereNotIn(Builder $query, $where)
    {
        if (!empty($where['values'])) {
            return $this->wrap($where['column']) . ' not in (' . $this->parameterize($where['values']) . ')';
        }

        return '1 = 1';
    }

    protected function whereNotInRaw(Builder $query, $where)
    {
        if (!empty($where['values'])) {
            return $this->wrap($where['column']) . ' not in (' . implode(', ', $where['values']) . ')';
        }

        return '1 = 1';
    }

    protected function whereInRaw(Builder $query, $where)
    {
        if (!empty($where['values'])) {
            return $this->wrap($where['column']) . ' in (' . implode(', ', $where['values']) . ')';
        }

        return '0 = 1';
    }


    protected function whereBetween(Builder $query, $where)
    {
        $between = $where['not'] ? 'not between' : 'between';

        $min = $this->parameter(reset($where['values']));

        $max = $this->parameter(end($where['values']));

        return $this->wrap($where['column']) . ' ' . $between . ' ' . $min . ' and ' . $max;
    }

    protected function whereBetweenColumns(Builder $query, $where)
    {
        $between = $where['not'] ? 'not between' : 'between';

        $min = $this->wrap(reset($where['values']));

        $max = $this->wrap(end($where['values']));

        return $this->wrap($where['column']) . ' ' . $between . ' ' . $min . ' and ' . $max;
    }

    protected function whereDate(Builder $query, $where)
    {
        return $this->dateBasedWhere('date', $query, $where);
    }

    protected function whereTime(Builder $query, $where)
    {
        return $this->dateBasedWhere('time', $query, $where);
    }

    protected function whereDay(Builder $query, $where)
    {
        return $this->dateBasedWhere('day', $query, $where);
    }

    protected function whereMonth(Builder $query, $where)
    {
        return $this->dateBasedWhere('month', $query, $where);
    }

    protected function whereYear(Builder $query, $where)
    {
        return $this->dateBasedWhere('year', $query, $where);
    }

    protected function dateBasedWhere($type, Builder $query, $where)
    {
        $value = $this->parameter($where['value']);

        return $type . '(' . $this->wrap($where['column']) . ') ' . $where['operator'] . ' ' . $value;
    }

    protected function whereColumn(Builder $query, $where)
    {
        return $this->wrap($where['first']) . ' ' . $where['operator'] . ' ' . $this->wrap($where['second']);
    }

    protected function whereNested(Builder $query, $where)
    {
        // Here we will calculate what portion of the string we need to remove. If this
        // is a join clause query, we need to remove the "on" portion of the SQL and
        // if it is a normal query we need to take the leading "where" of queries.
        $offset = $query instanceof JoinClause ? 3 : 6;

        return '(' . substr($this->compileWheres($where['query']), $offset) . ')';
    }

    protected function whereSub(Builder $query, $where)
    {
        $select = $this->compileSelect($where['query']);

        return $this->wrap($where['column']) . ' ' . $where['operator'] . " ($select)";
    }

    protected function whereExists(Builder $query, $where)
    {
        return 'exists (' . $this->compileSelect($where['query']) . ')';
    }

    protected function whereNotExists(Builder $query, $where)
    {
        return 'not exists (' . $this->compileSelect($where['query']) . ')';
    }

    protected function whereRowValues(Builder $query, $where)
    {
        $columns = $this->columnize($where['columns']);

        $values = $this->parameterize($where['values']);

        return '(' . $columns . ') ' . $where['operator'] . ' (' . $values . ')';
    }

    protected function compileGroups(Builder $query, $groups)
    {
        return 'group by ' . $this->columnize($groups);
    }

    protected function compileHavings(Builder $query, $havings)
    {
        $sql = implode(' ', array_map([$this, 'compileHaving'], $havings));

        return 'having ' . $this->removeLeadingBoolean($sql);
    }

    protected function compileHaving(array $having)
    {
        if ($having['type'] === 'Raw') {
            return $having['boolean'] . ' ' . $having['sql'];
        }

        if ($having['type'] === 'between') {
            return $this->compileHavingBetween($having);
        }

        return $this->compileBasicHaving($having);
    }

    protected function compileBasicHaving($having)
    {
        $column = $this->wrap($having['column']);

        $parameter = $this->parameter($having['value']);

        return $having['boolean'] . ' ' . $column . ' ' . $having['operator'] . ' ' . $parameter;
    }

    protected function compileHavingBetween($having)
    {
        $between = $having['not'] ? 'not between' : 'between';

        $column = $this->wrap($having['column']);

        return $having['boolean'] . ' ' . $column . ' ' . $between . ' ? and ?';
    }

    protected function compileOrders(Builder $query, $orders)
    {
        if (!empty($orders)) {
            return 'order by ' . implode(', ', $this->compileOrdersToArray($query, $orders));
        }

        return '';
    }

    protected function compileOrdersToArray(Builder $query, $orders)
    {
        return array_map(function ($order) {
            return $order['sql'] ?? $this->wrap($order['column']) . ' ' . $order['direction'];
        }, $orders);
    }


    protected function compileLimit(Builder $query, $limit)
    {
        return 'limit ' . (int)$limit;
    }

    protected function compileOffset(Builder $query, $offset)
    {
        return 'offset ' . (int)$offset;
    }

    protected function compileUnions(Builder $query)
    {
        $sql = '';

        foreach ($query->unions as $union) {
            $sql .= $this->compileUnion($union);
        }

        if (!empty($query->unionOrders)) {
            $sql .= ' ' . $this->compileOrders($query, $query->unionOrders);
        }

        if (isset($query->unionLimit)) {
            $sql .= ' ' . $this->compileLimit($query, $query->unionLimit);
        }

        if (isset($query->unionOffset)) {
            $sql .= ' ' . $this->compileOffset($query, $query->unionOffset);
        }

        return ltrim($sql);
    }

    protected function compileUnion(array $union)
    {
        $conjunction = $union['all'] ? ' union all ' : ' union ';

        return $conjunction . $this->wrapUnion($union['query']->toSql());
    }

    protected function wrapUnion($sql)
    {
        return '(' . $sql . ')';
    }

    protected function compileUnionAggregate(Builder $query)
    {
        $sql = $this->compileAggregate($query, $query->aggregate);

        $query->aggregate = null;

        return $sql . ' from (' . $this->compileSelect($query) . ') as ' . $this->wrapTable('temp_table');
    }

    public function compileExists(Builder $query)
    {
        $select = $this->compileSelect($query);

        return "select exists({$select}) as {$this->wrap('exists')}";
    }

    public function compileInsertUsing(Builder $query, array $columns, string $sql)
    {
        return "insert into {$this->wrapTable($query->from)} ({$this->columnize($columns)}) $sql";
    }

    protected function compileUpdateWithJoins(Builder $query, $table, $columns, $where)
    {
        $joins = $this->compileJoins($query, $query->joins);

        return "update {$table} {$joins} set {$columns} {$where}";
    }

    protected function compileUpdateWithoutJoins(Builder $query, $table, $columns, $where)
    {
        $sql = "update {$table} set {$columns} {$where}";

        if (!empty($query->orders)) {
            $sql .= ' ' . $this->compileOrders($query, $query->orders);
        }

        if (isset($query->limit)) {
            $sql .= ' ' . $this->compileLimit($query, $query->limit);
        }

        return $sql;
    }

    public function prepareBindingsForDelete(array $bindings)
    {
        unset($bindings['select']);
        return $bindings;
    }

    public function compileTruncate(Builder $query)
    {
        return ['truncate table ' . $this->wrapTable($query->from) => []];
    }


    public function supportsSavepoints()
    {
        return true;
    }

    public function compileSavepoint($name)
    {
        return 'SAVEPOINT ' . $name;
    }

    public function compileSavepointRollBack($name)
    {
        return 'ROLLBACK TO SAVEPOINT ' . $name;
    }

    public function wrap($value, $prefixAlias = false)
    {
        if ($this->isExpression($value)) {
            return $this->getValue($value);
        }

        if (stripos($value, ' as ') !== false) {
            return $this->wrapAliasedValue($value, $prefixAlias);
        }

        return $this->wrapSegments(explode('.', $value));
    }

    protected function wrapJsonBooleanValue($value)
    {
        return $value;
    }

    protected function wrapJsonFieldAndPath($column)
    {
        $parts = explode('->', $column, 2);

        $field = $this->wrap($parts[0]);

        $path = count($parts) > 1 ? ', ' . $this->wrapJsonPath($parts[1], '->') : '';

        return [$field, $path];
    }

    protected function wrapJsonPath($value, $delimiter = '->')
    {
        $value = preg_replace("/([\\\\]+)?\\'/", "''", $value);

        return '\'$."' . str_replace($delimiter, '"."', $value) . '"\'';
    }

    public function isJsonSelector($value)
    {
        return mb_strpos($value, '->') !== false;
    }

    protected function concatenate($segments)
    {
        return implode(' ', array_filter($segments, function ($value) {
            return (string)$value !== '';
        }));
    }

    public function isExpression($value)
    {
        return $value instanceof Expression;
    }

    public function getValue($expression)
    {
        return $expression->getValue();
    }

    protected function removeLeadingBoolean($value)
    {
        return preg_replace('/and |or /i', '', $value, 1);
    }

    public function getOperators()
    {
        return $this->operators;
    }

    public function getBitwiseOperators()
    {
        return $this->bitwiseOperators;
    }

    public function wrapArray(array $values)
    {
        return array_map([$this, 'wrap'], $values);
    }

    public function wrapTable($table)
    {
        if (!$this->isExpression($table)) {
            return $this->wrap($this->tablePrefix . $table, true);
        }

        return $this->getValue($table);
    }

    protected function wrapAliasedValue($value, $prefixAlias = false)
    {
        $segments = preg_split('/\s+as\s+/i', $value);

        if ($prefixAlias) {
            $segments[1] = $this->tablePrefix . $segments[1];
        }

        return $this->wrap($segments[0]) . ' as ' . $this->wrapValue($segments[1]);
    }

    protected function wrapSegments($segments)
    {
        $segments = (array)$segments;
        $keys = array_keys($segments);
        $callback = function ($segment, $key) use ($segments) {
            return $key == 0 && count($segments) > 1
                ? $this->wrapTable($segment)
                : $this->wrapValue($segment);
        };
        $items = array_map($callback, $segments, $keys);
        return implode('.', array_combine($keys, $items));
    }

    public function columnize(array $columns)
    {
        return implode(', ', array_map([$this, 'wrap'], $columns));
    }

    public function parameterize(array $values)
    {
        return implode(', ', array_map([$this, 'parameter'], $values));
    }

    public function parameter($value)
    {
        return $this->isExpression($value) ? $this->getValue($value) : '?';
    }

    public function quoteString($value)
    {
        if (is_array($value)) {
            return implode(', ', array_map([$this, __FUNCTION__], $value));
        }

        return "'$value'";
    }

    public function getTablePrefix()
    {
        return $this->tablePrefix;
    }

    public function setTablePrefix($prefix)
    {
        $this->tablePrefix = $prefix;

        return $this;
    }

    public function compileUpdate(Builder $query, array $values)
    {
        $table = $this->wrapTable($query->from);

        $columns = $this->compileUpdateColumns($query, $values);

        $where = $this->compileWheres($query);

        return trim(
            isset($query->joins)
                ? $this->compileUpdateWithJoins($query, $table, $columns, $where)
                : $this->compileUpdateWithoutJoins($query, $table, $columns, $where)
        );
    }

    public function compileBatchUpdate(Builder $query, array $values, string $index, bool $updateWithJoin = false)
    {
        $table = $this->wrapTable($query->from);

        $columns = $updateWithJoin
            ? $this->compileBatchUpdateWithJoinColumns($query, $values, $index)
            : $this->compileBatchUpdateColumns($query, $values, $index);

        $where = $this->compileWheres($query);

        return trim(
            isset($query->joins)
                ? $this->compileUpdateWithJoins($query, $table, $columns, $where)
                : $this->compileUpdateWithoutJoins($query, $table, $columns, $where)
        );
    }

    protected function compileUpdateColumns(Builder $query, array $values)
    {
        $keys = array_keys($values);
        $callback = function ($value, $key) {
            return $this->wrap($key) . ' = ' . $this->parameter($value);
        };
        $items = array_map($callback, $values, $keys);
        return implode(', ', array_combine($keys, $items));
    }

    protected function compileBatchUpdateColumns(Builder $query, array $values, string $index)
    {
        $columnCases = [];
        foreach ($values as $record) {
            foreach (array_keys($record) as $field) {
                if ($field === $index) {
                    continue;
                }
                $columnCases[$field][] = 'when ' . $this->wrap($index) . ' = ' . $this->parameter($record[$index]) . ' then ' . $this->parameter($record[$field]);
            }
        }

        $keys = array_keys($columnCases);
        $callback = function ($value, $key) {
            return $this->wrap($key) . ' = ' . '(case ' . implode("\n", $value) . ' else ' . $this->wrap($key) . ' end)';
        };
        $items = array_map($callback, $columnCases, $keys);
        return implode(', ', array_combine($keys, $items));
    }

    public function compileBatchUpdateWithJoinColumns(Builder $query, array $values, string $index)
    {
        $baseQuery = null;
        foreach ($values as $record) {
            $subQuery = $query->newQuery();
            $subQuery->selectRaw($this->compileBatchUpdateUnionColumns($subQuery, $record), array_values($record));
            if (null === $baseQuery) {
                $baseQuery = $subQuery;
            } else {
                $baseQuery->unionAll($subQuery);
            }
        }
        $query->joinSub($baseQuery, $as = 'updates', $query->from . '.' . $index, '=', $as . '.' . $index);

        $updateColumns = array_diff(array_keys($values[0]), [$index]);
        $fromTable = $query->from;
        $callback = function ($value) use ($fromTable, $as) {
            return $this->wrap($fromTable) . '.' . $this->wrap($value) . ' = ' . $this->wrap($as) . '.' . $this->wrap($value);
        };
        $items = array_map($callback, $updateColumns);
        return implode(', ', $items);
    }

    public function compileBatchUpdateUnionColumns(Builder $query, array $values)
    {
        $keys = array_keys($values);
        $callback = function ($value, $key) {
            return $this->parameter($value) . ' as ' . $this->wrap($key);
        };
        $items = array_map($callback, $values, $keys);
        return implode(', ', array_combine($keys, $items));
    }

    public function compileUpsert(Builder $query, array $values, array $uniqueBy, array $update)
    {
        $sql = $this->compileInsert($query, $values) . ' on duplicate key update ';

        $keys = array_keys($update);
        $callback = function ($value, $key) {
            return is_numeric($key)
                ? $this->wrap($value) . ' = values(' . $this->wrap($value) . ')'
                : $this->wrap($key) . ' = ' . $this->parameter($value);
        };
        $items = array_map($callback, $update, $keys);
        $columns = implode(', ', array_combine($keys, $items));

        return $sql . $columns;
    }

    protected function compileJsonUpdateColumn($key, $value)
    {
        if (is_bool($value)) {
            $value = $value ? 'true' : 'false';
        } elseif (is_array($value)) {
            $value = 'cast(? as json)';
        } else {
            $value = $this->parameter($value);
        }

        [$field, $path] = $this->wrapJsonFieldAndPath($key);

        return "{$field} = json_set({$field}{$path}, {$value})";
    }

    protected function wrapJsonSelector($value)
    {
        [$field, $path] = $this->wrapJsonFieldAndPath($value);

        return 'json_unquote(json_extract(' . $field . $path . '))';
    }

    /**
     * Wrap the given JSON selector for boolean values.
     *
     * @param string $value
     * @return string
     */
    protected function wrapJsonBooleanSelector($value)
    {
        [$field, $path] = $this->wrapJsonFieldAndPath($value);

        return 'json_extract(' . $field . $path . ')';
    }
}
