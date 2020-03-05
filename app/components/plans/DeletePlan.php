<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
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

    /** @var Condition|ConditionTree|null  */
    protected $condition;

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
     * @return array|mixed
     * @throws \Throwable
     */
    public function execute()
    {
        $rows = $this->query();

        foreach ($this->schemas as $schema) {
            $table = $schema['table'];
            $schemaMeta = $this->storage->getSchemaMetaData($table);
            $pkList = array_column($rows, $table . '.' . $schemaMeta['pk']);
            $this->storage->del($table, $pkList);
        }

        return 1;

        //todo 查询数据主键，批量删除

    }
}
