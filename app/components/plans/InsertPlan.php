<?php

namespace App\components\plans;

use App\components\Ast;
use App\components\elements\Column;
use App\components\storage\AbstractStorage;

class InsertPlan
{
    /** @var Ast */
    protected $ast;

    /** @var AbstractStorage */
    protected $storage;

    /** @var Column[] */
    protected $columns = [];

    protected $schemas;

    public function __construct(Ast $ast, AbstractStorage $storage)
    {
        $this->ast = $ast;

        //todo sql校验
    }

    /**
     * @return array|mixed
     * @throws \Throwable
     */
    public function execute()
    {
        //todo
    }
}
