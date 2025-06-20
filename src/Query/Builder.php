<?php

namespace GodJarvis\Database\Query;

use Closure;
use InvalidArgumentException;

class Builder
{
    public $sql = '';

    public $operate = 'select';

    public $updateOrInsertValues = [];

    public $upsertValues = [
        'insert' => [],
        'unique_by' => [],
        'update' => []
    ];

    public $batchUpdateValues = [
        'values' => [],
        'index' => 'id',
        'updateWithJoin' => false,
    ];

    public $grammar;

    public $bindings = [
        'select' => [],
        'from' => [],
        'join' => [],
        'where' => [],
        'groupBy' => [],
        'having' => [],
        'order' => [],
        'union' => [],
        'unionOrder' => [],
    ];

    public $aggregate;

    public $columns;

    public $distinct = false;

    public $from;

    public $joins;

    public $wheres = [];

    public $groups;

    public $havings;

    public $orders;

    public $limit;

    public $offset;

    public $unions;

    public $unionLimit;

    public $unionOffset;

    public $unionOrders;

    public $operators = [
        '=',
        '<',
        '>',
        '<=',
        '>=',
        '<>',
        '!=',
        '<=>',
        'like',
        'like binary',
        'not like',
        'ilike',
        '&',
        '|',
        '^',
        '<<',
        '>>',
        '&~',
        'rlike',
        'not rlike',
        'regexp',
        'not regexp',
        '~',
        '~*',
        '!~',
        '!~*',
        'similar to',
        'not similar to',
        'not ilike',
        '~~*',
        '!~~*',
    ];

    public $bitwiseOperators = [
        '&',
        '|',
        '^',
        '<<',
        '>>',
        '&~',
    ];


    public function __construct()
    {
        $this->grammar = new Grammar();
    }

    public function select($columns = ['*'])
    {
        $this->bindings['select'] = [];
        $this->columns = $columns;

        return $this;
    }

    public function from($table, $as = null)
    {
        $this->from = $as ? "{$table} as {$as}" : $table;

        return $this;
    }

    public function join($table, $first, $operator = null, $second = null, $type = 'inner', $where = false)
    {
        $join = $this->newJoinClause($this, $type, $table);

        if ($first instanceof Closure) {
            $first($join);

            $this->joins[] = $join;
        } else {
            $method = $where ? 'where' : 'on';

            $this->joins[] = $join->$method($first, $operator, $second);
        }
        $this->addBinding($join->getBindings(), 'join');

        return $this;
    }

    public function joinSub($query, $as, $first, $operator = null, $second = null, $type = 'inner', $where = false)
    {
        [$query, $bindings] = $this->createSub($query);

        $expression = '(' . $query . ') as ' . $this->grammar->wrapTable($as);

        $this->addBinding($bindings, 'join');

        return $this->join(new Expression($expression), $first, $operator, $second, $type, $where);
    }

    public function on($first, $operator = null, $second = null, $boolean = 'and')
    {
        return $this->whereColumn($first, $operator, $second, $boolean);
    }

    public function leftJoin($table, $first, $operator = null, $second = null)
    {
        return $this->join($table, $first, $operator, $second, 'left');
    }

    public function leftJoinSub($query, $as, $first, $operator = null, $second = null)
    {
        return $this->joinSub($query, $as, $first, $operator, $second, 'left');
    }

    public function rightJoin($table, $first, $operator = null, $second = null)
    {
        return $this->join($table, $first, $operator, $second, 'right');
    }

    public function rightJoinSub($query, $as, $first, $operator = null, $second = null)
    {
        return $this->joinSub($query, $as, $first, $operator, $second, 'right');
    }

    public function where($column, $operator = null, $value = null, $boolean = 'and')
    {
        if (is_array($column)) {
            return $this->addArrayOfWheres($column, $boolean);
        }

        [$value, $operator] = $this->prepareValueAndOperator(
            $value,
            $operator,
            func_num_args() === 2
        );

        if ($column instanceof Closure && is_null($operator)) {
            return $this->whereNested($column, $boolean);
        }

        if (!is_null($operator) && $this->isQueryable($column)) {
            [$sub, $bindings] = $this->createSub($column);

            return $this->addBinding($bindings)
                ->where(new Expression('(' . $sub . ')'), $operator, $value, $boolean);
        }

        if ($this->invalidOperator($operator)) {
            [$value, $operator] = [$operator, '='];
        }

        if ($value instanceof Closure) {
            return $this->whereSub($column, $operator, $value, $boolean);
        }

        if (is_null($value)) {
            return $this->whereNull($column, $boolean, $operator !== '=');
        }

        $type = 'Basic';

        if (is_bool($value) && mb_strpos($column, '->') !== false) {
            $value = new Expression($value ? 'true' : 'false');

            if (is_string($column)) {
                $type = 'JsonBoolean';
            }
        }

        if ($this->isBitwiseOperator($operator)) {
            $type = 'Bitwise';
        }

        $this->wheres[] = compact(
            'type',
            'column',
            'operator',
            'value',
            'boolean'
        );

        if (!$value instanceof Expression) {
            $this->addBinding($value);
        }

        return $this;
    }

    protected function addArrayOfWheres($column, $boolean, $method = 'where')
    {
        return $this->whereNested(function ($query) use ($column, $method, $boolean) {
            foreach ($column as $key => $value) {
                if (is_numeric($key) && is_array($value)) {
                    $query->{$method}(...array_values($value));
                } else {
                    $query->$method($key, '=', $value, $boolean);
                }
            }
        }, $boolean);
    }

    public function whereNested(Closure $callback, $boolean = 'and')
    {
        $callback($query = $this->forNestedWhere());

        return $this->addNestedWhereQuery($query, $boolean);
    }

    public function forNestedWhere()
    {
        return $this->newQuery()->from($this->from);
    }

    public function addNestedWhereQuery(Builder $query, $boolean = 'and')
    {
        if (count($query->wheres)) {
            $type = 'Nested';

            $this->wheres[] = compact('type', 'query', 'boolean');

            $this->addBinding($query->getRawBindings()['where']);
        }

        return $this;
    }

    protected function whereSub($column, $operator, Closure $callback, $boolean)
    {
        $type = 'Sub';

        $callback($query = $this->forSubQuery());

        $this->wheres[] = compact(
            'type', 'column', 'operator', 'query', 'boolean'
        );

        $this->addBinding($query->getBindings());

        return $this;
    }

    public function whereExists(Closure $callback, $boolean = 'and', $not = false)
    {
        $query = $this->forSubQuery();

        $callback($query);

        return $this->addWhereExistsQuery($query, $boolean, $not);
    }

    public function orWhereExists(Closure $callback, bool $not = false)
    {
        return $this->whereExists($callback, 'or', $not);
    }

    public function whereNotExists(Closure $callback, $boolean = 'and')
    {
        return $this->whereExists($callback, $boolean, true);
    }

    public function orWhereNotExists(Closure $callback)
    {
        return $this->orWhereExists($callback, true);
    }

    public function addWhereExistsQuery(self $query, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotExists' : 'Exists';

        $this->wheres[] = compact('type', 'query', 'boolean');

        $this->addBinding($query->getBindings());

        return $this;
    }

    public function getRawBindings()
    {
        return $this->bindings;
    }

    public function whereColumn($first, $operator = null, $second = null, $boolean = 'and')
    {
        if ($this->invalidOperator($operator)) {
            [$second, $operator] = [$operator, '='];
        }

        $type = 'Column';

        $this->wheres[] = compact(
            'type',
            'first',
            'operator',
            'second',
            'boolean'
        );

        return $this;
    }

    public function prepareValueAndOperator($value, $operator, $useDefault = false)
    {
        if ($useDefault) {
            return [$operator, '='];
        }

        if ($this->invalidOperatorAndValue($operator, $value)) {
            throw new InvalidArgumentException('Illegal operator and value combination.');
        }

        return [$value, $operator];
    }

    protected function invalidOperatorAndValue($operator, $value)
    {
        return is_null($value) && in_array($operator, $this->operators, true) &&
            !in_array($operator, ['=', '<>', '!=']);
    }

    protected function invalidOperator($operator)
    {
        return !is_string($operator) || (!in_array(strtolower($operator), $this->operators, true) &&
                !in_array(strtolower($operator), $this->grammar->getOperators(), true));
    }

    protected function isBitwiseOperator($operator)
    {
        return in_array(strtolower($operator), $this->bitwiseOperators, true) ||
            in_array(strtolower($operator), $this->grammar->getBitwiseOperators(), true);
    }

    public function orWhere($column, $operator = null, $value = null)
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value,
            $operator,
            func_num_args() === 2
        );

        return $this->where($column, $operator, $value, 'or');
    }

    public function whereRaw($sql, $bindings = [], $boolean = 'and')
    {
        $this->wheres[] = ['type' => 'raw', 'sql' => $sql, 'boolean' => $boolean];

        $this->addBinding((array)$bindings);

        return $this;
    }

    public function orWhereRaw($sql, $bindings = [])
    {
        return $this->whereRaw($sql, $bindings, 'or');
    }

    public function whereIn($column, $values, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotIn' : 'In';

        if ($this->isQueryable($values)) {
            [$query, $bindings] = $this->createSub($values);

            $values = [new Expression($query)];

            $this->addBinding($bindings);
        }

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');

        $this->addBinding($this->cleanBindings($values));

        return $this;
    }

    public function orWhereIn($column, $values)
    {
        return $this->whereIn($column, $values, 'or');
    }

    public function whereNotIn($column, $values, $boolean = 'and')
    {
        return $this->whereIn($column, $values, $boolean, true);
    }

    public function orWhereNotIn($column, $values)
    {
        return $this->whereNotIn($column, $values, 'or');
    }

    public function whereNull($columns, $boolean = 'and', $not = false)
    {
        $type = $not ? 'NotNull' : 'Null';

        foreach ((array)($columns) as $column) {
            $this->wheres[] = compact('type', 'column', 'boolean');
        }

        return $this;
    }

    public function orWhereNull($column)
    {
        return $this->whereNull($column, 'or');
    }

    public function whereNotNull($columns, $boolean = 'and')
    {
        return $this->whereNull($columns, $boolean, true);
    }

    public function whereBetween($column, array $values, $boolean = 'and', $not = false)
    {
        $type = 'between';

        $this->wheres[] = compact('type', 'column', 'values', 'boolean', 'not');

        $this->addBinding(array_slice($values, 0, 2));

        return $this;
    }

    public function orWhereBetween($column, array $values)
    {
        return $this->whereBetween($column, $values, 'or');
    }

    public function whereNotBetween($column, array $values, $boolean = 'and')
    {
        return $this->whereBetween($column, $values, $boolean, true);
    }

    public function orWhereNotBetween($column, array $values)
    {
        return $this->whereNotBetween($column, $values, 'or');
    }

    public function orWhereNotNull($column)
    {
        return $this->whereNotNull($column, 'or');
    }

    public function groupBy(...$groups)
    {
        foreach ($groups as $group) {
            $this->groups = array_merge(
                (array)$this->groups,
                (array)($group)
            );
        }

        return $this;
    }

    public function having($column, $operator = null, $value = null, $boolean = 'and')
    {
        $type = 'Basic';

        [$value, $operator] = $this->prepareValueAndOperator(
            $value,
            $operator,
            func_num_args() === 2
        );

        if ($this->invalidOperator($operator)) {
            [$value, $operator] = [$operator, '='];
        }

        if ($this->isBitwiseOperator($operator)) {
            $type = 'Bitwise';
        }

        $this->havings[] = compact('type', 'column', 'operator', 'value', 'boolean');

        $this->addBinding($value, 'having');

        return $this;
    }

    public function orderBy($column, $direction = 'asc')
    {
        $direction = strtolower($direction);

        if (!in_array($direction, ['asc', 'desc'], true)) {
            throw new InvalidArgumentException('Order direction must be "asc" or "desc".');
        }

        $this->orders[] = [
            'column' => $column,
            'direction' => $direction,
        ];

        return $this;
    }

    public function orderByRaw($sql, $bindings = [])
    {
        $type = 'Raw';

        $this->{$this->unions ? 'unionOrders' : 'orders'}[] = compact('type', 'sql');

        $this->addBinding($bindings, $this->unions ? 'unionOrder' : 'order');

        return $this;
    }

    public function offset($value)
    {
        $this->offset = max(0, (int)$value);

        return $this;
    }

    public function limit($value)
    {
        if ($value >= 0) {
            $this->limit = !is_null($value) ? (int)$value : null;
        }

        return $this;
    }

    public function forPage($page, $perPage = 15)
    {
        return $this->offset(($page - 1) * $perPage)->limit($perPage);
    }

    public function union($query, $all = false)
    {
        if ($query instanceof Closure) {
            $query($query = $this->newQuery());
        }

        $this->unions[] = compact('query', 'all');

        $this->addBinding($query->getBindings(), 'union');

        return $this;
    }

    public function unionAll($query)
    {
        return $this->union($query, true);
    }

    public function newQuery()
    {
        return new static();
    }

    public function toSql()
    {
        switch (true) {
            case 'select' === $this->operate:
                $this->sql = $this->grammar->compileSelect($this);
                break;
            case 'update' === $this->operate:
                $this->sql = $this->grammar->compileUpdate($this, $this->updateOrInsertValues);
                break;
            case 'batchUpdate' === $this->operate:
                $this->sql = $this->grammar->compileBatchUpdate($this, $this->batchUpdateValues['values'], $this->batchUpdateValues['index'], $this->batchUpdateValues['updateWithJoin']);
                break;
            case 'insert' === $this->operate:
                $this->sql = $this->grammar->compileInsert($this, $this->updateOrInsertValues);
                break;
            case 'insertOrIgnore' === $this->operate:
                $this->sql = $this->grammar->compileInsertOrIgnore($this, $this->updateOrInsertValues);
                break;
            case 'upsert' === $this->operate:
                $this->sql = $this->grammar->compileUpsert($this, $this->upsertValues['insert'], (array)$this->upsertValues['unique_by'], $this->upsertValues['update']);
                break;
            case 'delete' === $this->operate:
                $this->sql = $this->grammar->compileDelete($this);
                break;
        }
        return $this->sql;
    }

    public function getBindings()
    {
        $bindValues = [];
        switch (true) {
            case 'select' === $this->operate:
                $bindValues = self::flatten($this->bindings);
                break;
            case 'update' === $this->operate:
                $bindValues = $this->prepareBindingsForUpdate($this->bindings, $this->updateOrInsertValues);
                break;
            case 'batchUpdate' === $this->operate:
                $bindValues = $this->prepareBindingsForBatchUpdate($this->bindings, $this->batchUpdateValues['values'], $this->batchUpdateValues['index'], $this->batchUpdateValues['updateWithJoin']);
                break;
            case 'insert' === $this->operate:
            case 'insertOrIgnore' === $this->operate:
                $bindValues = $this->prepareBindingsForInsert($this->updateOrInsertValues);
                break;
            case 'upsert' === $this->operate:
                $bindValues = $this->prepareBindingsForUpsert($this->upsertValues['insert'], $this->upsertValues['update']);
                break;
            case 'delete' === $this->operate:
                $bindValues = $this->prepareBindingsForDelete($this->updateOrInsertValues);
                break;
        }
        return $bindValues;
    }

    public function update(array $values)
    {
        $this->operate = 'update';
        $this->updateOrInsertValues = $values;
        return $this;
    }

    public function batchUpdate(array $values, string $index, bool $updateWithJoin = false)
    {
        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }
        $this->whereIn($index, array_column($values, $index));
        $this->operate = 'batchUpdate';
        $this->batchUpdateValues['values'] = $values;
        $this->batchUpdateValues['index'] = $index;
        $this->batchUpdateValues['updateWithJoin'] = $updateWithJoin;
        return $this;
    }

    public function upsert(array $values, $uniqueBy, $update = null)
    {
        if ($update === []) {
            return $this->insert($values);
        }

        $this->operate = 'upsert';
        $this->upsertValues['unique_by'] = $uniqueBy;
        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);

                $values[$key] = $value;
            }
        }
        $this->upsertValues['insert'] = $values;

        if (is_null($update)) {
            $update = array_keys(reset($values));
        }
        $this->upsertValues['update'] = $update;
        return $this;
    }

    public function prepareBindingsForUpdate(array $bindings, array $values)
    {
        $cleanBindings = $bindings;
        unset($cleanBindings['select'], $cleanBindings['join']);

        return array_values(
            array_merge($bindings['join'], $values, self::flatten($cleanBindings))
        );
    }

    public function prepareBindingsForBatchUpdate(array $bindings, array $values, string $index, bool $updateWithJoin = false)
    {
        return $updateWithJoin
            ? $this->prepareBindingsForBatchUpdateWithJoin($bindings, $values)
            : $this->prepareBindingsForBatchUpdateWithCaseWhen($bindings, $values, $index);
    }

    public function prepareBindingsForBatchUpdateWithCaseWhen(array $bindings, array $values, string $index)
    {
        $cleanBindings = $bindings;
        unset($cleanBindings['select'], $cleanBindings['join']);

        $caseValues = [];
        foreach ($values as $record) {
            foreach (array_keys($record) as $field) {
                if ($field === $index) {
                    continue;
                }
                $caseValues[$field][] = [$record[$index], $record[$field]];
            }
        }
        $values = self::flatten($caseValues);

        return array_values(
            array_merge($bindings['join'], $values, self::flatten($cleanBindings))
        );
    }

    public function prepareBindingsForBatchUpdateWithJoin(array $bindings, array $values)
    {
        $cleanBindings = $bindings;
        unset($cleanBindings['select'], $cleanBindings['join']);

        return array_values(
            array_merge($bindings['join'], self::flatten($cleanBindings))
        );
    }

    public function prepareBindingsForDelete(array $bindings)
    {
        $cleanBindings = $bindings;
        unset($cleanBindings['select']);

        return self::flatten($cleanBindings);
    }

    public function insert(array $values)
    {
        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        $this->operate = 'insert';
        $this->updateOrInsertValues = $values;
        return $this;
    }

    public function insertOrIgnore(array $values)
    {
        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        $this->operate = 'insertOrIgnore';
        $this->updateOrInsertValues = $values;
        return $this;
    }

    public function delete($id = null)
    {
        if (!is_null($id)) {
            $this->where($this->from . '.id', '=', $id);
        }

        $this->operate = 'delete';
        return $this;
    }

    public function prepareBindingsForInsert(array $values)
    {
        return self::flatten($values, 1);
    }

    public function prepareBindingsForUpsert(array $values, array $update)
    {
        return array_merge(
            self::flatten($values, 1),
            array_filter($update, function ($value, $key) {
                return !is_int($key);
            }, ARRAY_FILTER_USE_BOTH)
        );
    }

    public function toFullSql()
    {
        $sql = $this->sql;
        foreach ($this->getBindings() as $key => $value) {
            $position = strpos($sql, '?');
            if ($position !== false) {
                $sql = substr_replace($sql, "'{$value}'", $position, 1);
            }
        }
        return $sql;
    }

    public function addBinding($value, $type = 'where')
    {
        if (!array_key_exists($type, $this->bindings)) {
            throw new InvalidArgumentException("Invalid binding type: {$type}.");
        }

        if (is_array($value)) {
            $this->bindings[$type] = array_values(array_merge($this->bindings[$type], $value));
        } else {
            $this->bindings[$type][] = $value;
        }

        return $this;
    }

    protected function newJoinClause(self $parentQuery, $type, $table)
    {
        return new JoinClause($parentQuery, $type, $table);
    }

    public function getGrammar()
    {
        return $this->grammar;
    }

    public static function flatten($array, $depth = INF)
    {
        $result = [];
        foreach ($array as $item) {
            if (!is_array($item)) {
                $result[] = $item;
            } else {
                $values = $depth === 1
                    ? array_values($item)
                    : static::flatten($item, $depth - 1);

                foreach ($values as $value) {
                    $result[] = $value;
                }
            }
        }

        return $result;
    }

    public function cleanBindings(array $bindings)
    {
        return array_filter($bindings, function ($val) {
            return !$val instanceof Expression;
        });
    }

    public function selectRaw($expression, array $bindings = [])
    {
        $this->addSelect(new Expression($expression));

        if ($bindings) {
            $this->addBinding($bindings, 'select');
        }

        return $this;
    }

    protected function createSub($query)
    {
        if ($query instanceof Closure) {
            $callback = $query;

            $callback($query = $this->forSubQuery());
        }

        return $this->parseSub($query);
    }

    protected function forSubQuery()
    {
        return $this->newQuery();
    }

    protected function parseSub($query)
    {
        if ($query instanceof self) {
            return [$query->toSql(), $query->getBindings()];
        }

        if (is_string($query)) {
            return [$query, []];
        }

        throw new InvalidArgumentException(
            'A subquery must be a query builder instance, a Closure, or a string.'
        );
    }

    public function addSelect($column)
    {
        $columns = is_array($column) ? $column : func_get_args();

        foreach ($columns as $as => $column) {
            $this->columns[] = $column;
        }

        return $this;
    }

    public function aggregate($function, $columns = ['*'], $asAlias = 'aggregate')
    {
        return $this->cloneWithout($this->unions || $this->havings ? [] : ['columns'])
            ->cloneWithoutBindings($this->unions || $this->havings ? [] : ['select'])
            ->setAggregate($function, $columns, $asAlias);
    }

    public function cloneWithout(array $properties)
    {
        (function ($clone) use ($properties) {
            foreach ($properties as $property) {
                $clone->{$property} = null;
            }
        })($clone = $this->clone());
        return $clone;
    }

    public function clone()
    {
        return clone $this;
    }

    protected function isQueryable($value)
    {
        return $value instanceof self ||
            $value instanceof Closure;
    }

    public function cloneWithoutBindings(array $except)
    {
        (function ($clone) use ($except) {
            foreach ($except as $type) {
                $clone->bindings[$type] = [];
            }
        })($clone = $this->clone());
        return $clone;
    }

    protected function setAggregate($function, $columns, $asAlias)
    {
        $this->aggregate = compact('function', 'columns', 'asAlias');

        if (empty($this->groups)) {
            $this->orders = null;

            $this->bindings['order'] = [];
        }

        return $this;
    }

    public function count($columns = '*', $asAlias = 'aggregate')
    {
        return $this->aggregate(__FUNCTION__, (array)$columns, $asAlias);
    }

    public function min($column, $asAlias = 'aggregate')
    {
        return $this->aggregate(__FUNCTION__, [$column], $asAlias);
    }

    public function max($column, $asAlias = 'aggregate')
    {
        return $this->aggregate(__FUNCTION__, [$column], $asAlias);
    }

    public function sum($column, $asAlias = 'aggregate')
    {
        return $this->aggregate(__FUNCTION__, [$column], $asAlias);
    }

    public function avg($column, $asAlias = 'aggregate')
    {
        return $this->aggregate(__FUNCTION__, [$column], $asAlias);
    }
}
