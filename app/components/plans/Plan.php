<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\consts\StmtType;

class Plan
{
    const STMT_TYPE_PLAN_MAPPING = [
        StmtType::SELECT => QueryPlan::class,
    ];

    /** @var Ast */
    protected $ast;

    protected $executePlan;

    public static function fromAst(Ast $ast)
    {
        return new static($ast);
    }

    public function __construct($ast)
    {
        $this->ast = $ast;
        $this->generatePlan();
    }

    protected function generatePlan()
    {
        $planClass = self::STMT_TYPE_PLAN_MAPPING[$this->ast->getStmtType()];
        $this->executePlan = new $planClass($this->ast);
    }

    public function execute($storage)
    {
        return $this->executePlan->execute($storage);
    }
}
