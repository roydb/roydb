<?php

namespace App\components;

use App\components\consts\StmtType;

class Ast
{
    private $sql;

    private $stmt;

    private $stmtType;

    public function __construct($sql, $stmt)
    {
        $this->sql = $sql;
        $this->stmt = $stmt;
        $this->parseStmtType();
    }

    protected function parseStmtType()
    {
        if (array_key_exists('SELECT', $this->stmt)) {
            $this->stmtType = StmtType::SELECT;
        } elseif (array_key_exists('INSERT', $this->stmt)) {
            $this->stmtType = StmtType::INSERT;
        } elseif (array_key_exists('DELETE', $this->stmt)) {
            $this->stmtType = StmtType::DELETE;
        } elseif (array_key_exists('UPDATE', $this->stmt)) {
            $this->stmtType = StmtType::UPDATE;
        }
    }

    /**
     * @return mixed
     */
    public function getSql()
    {
        return $this->sql;
    }

    /**
     * @return mixed
     */
    public function getStmt()
    {
        return $this->stmt;
    }

    /**
     * @return mixed
     */
    public function getStmtType()
    {
        return $this->stmtType;
    }
}
