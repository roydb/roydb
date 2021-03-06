<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\optimizers\CostBasedOptimizer;
use App\components\optimizers\RulesBasedOptimizer;
use App\components\storage\AbstractStorage;
use PHPSQLParser\PHPSQLCreator;

class DeletePlan
{
    /** @var Ast */
    protected $ast;

    /** @var AbstractStorage */
    protected $storage;

    protected $schemas;

    /**
     * DeletePlan constructor.
     * @param Ast $ast
     * @param AbstractStorage $storage
     * @throws \Exception
     */
    public function __construct(Ast $ast, AbstractStorage $storage)
    {
        $this->ast = $ast;
        $this->storage = $storage;
        $this->extractSchemas();
    }

    protected function extractSchemas()
    {
        $stmt = $this->ast->getStmt();
        if (isset($stmt['FROM'])) {
            $this->schemas = $stmt['FROM'];
        }
    }

    protected function query()
    {
        $stmt = $this->ast->getStmt();

        unset($stmt['DELETE']);

        $queryStmt = [];

        $schemas = [];
        foreach ($this->schemas as $schema) {
            $table = $schema['table'];
            $schemaMeta = $this->storage->getSchemaMetaData($table);
            $schemas[] = [
                'expr_type' => 'colref',
                'alias' => false,
                'base_expr' => $table . '.' . $schemaMeta['pk'],
                'sub_tree' => false,
                'delim' => false,
            ];
        }
        $queryStmt['SELECT'] = $schemas;
        $queryStmt = array_merge($queryStmt, $stmt);

        $queryAst = new Ast((new PHPSQLCreator())->create($queryStmt), $queryStmt);
        $plan = Plan::create($queryAst, $this->storage);
        $plan = RulesBasedOptimizer::fromPlan($plan)->optimize();
        $plan = CostBasedOptimizer::fromPlan($plan)->optimize();
        return $plan->execute();
    }

    /**
     * @return int
     * @throws \Exception
     */
    public function execute()
    {
        $rows = $this->query();
        $deleted = 0;

        foreach ($this->schemas as $schema) {
            $table = $schema['table'];
            $schemaMeta = $this->storage->getSchemaMetaData($table);
            if (is_null($schemaMeta)) {
                throw new \Exception('Schema ' . $table . ' not exists');
            }

            $pkList = array_column($rows, $table . '.' . $schemaMeta['pk']);
            $deleted += $this->storage->del($table, $pkList);
        }

        return $deleted;

    }
}
