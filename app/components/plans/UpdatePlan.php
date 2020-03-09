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

    /** @var Condition|ConditionTree|null */
    protected $condition;

    protected $sets;
    protected $updateRow;

    /**
     * UpdatePlan constructor.
     * @param Ast $ast
     * @param AbstractStorage $storage
     * @throws \Exception
     */
    public function __construct(Ast $ast, AbstractStorage $storage)
    {
        $this->ast = $ast;
        $this->storage = $storage;
        $this->extractSchemas();
        $this->extractSets();
    }

    protected function extractSchemas()
    {
        $stmt = $this->ast->getStmt();
        if (isset($stmt['UPDATE'])) {
            $this->schemas = $stmt['UPDATE'];
        }
    }

    /**
     * @throws \Exception
     */
    protected function extractSets()
    {
        $stmt = $this->ast->getStmt();
        if (!isset($stmt['SET'])) {
            throw new \Exception('Missing values in the sql');
        }

        $this->sets = $stmt['SET'];

        $updateRow = [];

        foreach ($this->sets as $set) {
            $key = $set['sub_tree'][0]['base_expr'];
            $value = $set['sub_tree'][2]['base_expr'];

            $isString = false;
            if (strpos($value, '"') === 0) {
                $value = substr($value, 1);
                $isString = true;
            }
            if (strpos($value, '"') === (strlen($value) - 1)) {
                $value = substr($value, 0, -1);
                $isString = true;
            }

            if (!$isString) {
                if (ctype_digit($value)) {
                    $value = intval($value);
                } elseif (is_numeric($value) && (strpos($value, '.') !== false)) {
                    $value = doubleval($value);
                } elseif ($value === 'true') {
                    $value = true;
                } elseif ($value === 'false') {
                    $value = false;
                } elseif ($value === 'null') {
                    $value = null;
                }
            }

            $updateRow[$key] = $value;
        }

        $this->updateRow = $updateRow;
    }

    /**
     * @return array|mixed
     * @throws \PHPSQLParser\exceptions\UnsupportedFeatureException
     * @throws \Throwable
     */
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

    protected function extractUpdateRow($schema)
    {
        //todo
        $rows = [];
        return $rows;
    }

    /**
     * @throws \PHPSQLParser\exceptions\UnsupportedFeatureException
     * @throws \Throwable
     */
    public function execute()
    {
        $affectedRows = 0;

        $rows = $this->query();

        foreach ($this->schemas as $schema) {
            $table = $schema['table'];

            $schemaMeta = $this->storage->getSchemaMetaData($table);
            $pkList = array_column($rows, $table . '.' . $schemaMeta['pk']);

            $affectedRows += $this->storage->update(
                $table,
                $pkList,
                $this->extractUpdateRow($table)
            );
        }
        die;
    }
}
