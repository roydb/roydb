<?php

namespace App\components\plans;

use App\components\Ast;

class QueryPlan
{
    /** @var Ast */
    protected $ast;

    public function __construct($ast)
    {
        $this->ast = $ast;
    }

    public function execute($storage)
    {
        $stmt = $this->ast->getStmt();

        $schema = $stmt['FROM'][0]['table'];

        $conditions = $stmt['WHERE'];
        $idxField = $conditions[0]['base_expr'];
        $idxValue = $conditions[2]['base_expr'];

        $columns = array_column($stmt['SELECT'], 'base_expr');

        return $storage->get($schema, [$idxField => $idxValue], $columns);
    }
}
