<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\optimizers\CostBasedOptimizer;
use App\components\optimizers\RulesBasedOptimizer;
use App\components\storage\AbstractStorage;
use PHPSQLParser\PHPSQLCreator;

class UpdatePlan
{
    //todo

    /** @var Ast */
    protected $ast;

    /** @var AbstractStorage */
    protected $storage;

    protected $schemas;

    /** @var Condition|ConditionTree|null  */
    protected $condition;

    protected $values;
    protected $rows;

    /**
     * UpdatePlan constructor.
     * @param Ast $ast
     * @param AbstractStorage $storage
     */
    public function __construct(Ast $ast, AbstractStorage $storage)
    {
        $this->ast = $ast;
        $this->storage = $storage;
        $this->extractSchemas();
        $this->extractValues();
    }

    protected function extractSchemas()
    {
        $stmt = $this->ast->getStmt();
        if (isset($stmt['UPDATE'])) {
            $this->schemas = $stmt['UPDATE'];
        }
    }

    protected function extractValues()
    {
        //todo
    }

    protected function query()
    {
        $stmt = $this->ast->getStmt();

        unset($stmt['SET']);

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
        $queryStmt['FROM'] = $stmt['UPDATE'];
        unset($stmt['UPDATE']);
        $queryStmt = array_merge($queryStmt, $stmt);

        $queryAst = new Ast((new PHPSQLCreator())->create($queryStmt), $queryStmt);
        $plan = Plan::create($queryAst, $this->storage);
        $plan = RulesBasedOptimizer::fromPlan($plan)->optimize();
        $plan = CostBasedOptimizer::fromPlan($plan)->optimize();
        return $plan->execute();
    }

    /**
     * @return int
     */
    public function execute()
    {
        var_dump($this->query());
        die;
    }
}
