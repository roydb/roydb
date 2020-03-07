<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\elements\condition\Condition;
use App\components\elements\condition\ConditionTree;
use App\components\storage\AbstractStorage;

class UpdatePlan
{
    //todo

    /** @var Ast */
    protected $ast;

    /** @var AbstractStorage */
    protected $storage;

    /** @var Condition|ConditionTree|null  */
    protected $condition;

    /**
     * UpdatePlan constructor.
     * @param Ast $ast
     * @param AbstractStorage $storage
     */
    public function __construct(Ast $ast, AbstractStorage $storage)
    {
        $this->ast = $ast;
        $this->storage = $storage;
    }
}
