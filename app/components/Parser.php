<?php

namespace App\components;

use PHPSQLParser\PHPSQLParser;

class Parser
{
    private $sql;

    public static function fromSql($sql)
    {
        return (new static($sql))->parseAst();
    }

    public function __construct($sql)
    {
        $this->sql = $sql;
    }

    public function parseAst()
    {
        return new Ast($this->sql, (new PHPSQLParser())->parse($this->sql));
    }
}
