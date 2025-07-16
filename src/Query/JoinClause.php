<?php

namespace GodJarvis\Database\Query;

class JoinClause extends Builder
{
    public $type;

    public $table;

    protected $parentConnection;

    protected $parentGrammar;

    protected $parentProcessor;

    protected $parentClass;

    public function __construct(Builder $parentQuery, $type, $table)
    {
        $this->type = $type;
        $this->table = $table;
        $this->parentClass = get_class($parentQuery);
        $this->parentGrammar = $parentQuery->getGrammar();

        parent::__construct();
    }

    public function on($first, $operator = null, $second = null, $boolean = 'and')
    {
        return $this->whereColumn($first, $operator, $second, $boolean);
    }

    public function newQuery()
    {
        return new static($this->newParentQuery(), $this->type, $this->table);
    }

    protected function forSubQuery()
    {
        return $this->newParentQuery()->newQuery();
    }

    protected function newParentQuery()
    {
        $class = $this->parentClass;

        return new $class($this->parentConnection, $this->parentGrammar, $this->parentProcessor);
    }
}
