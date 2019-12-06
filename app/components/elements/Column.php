<?php

namespace App\components\elements;

class Column
{
    //todo support agg function

    protected $type;

    protected $value;

    /** @var Column[]  */
    protected $subColumns = [];

    /**
     * @param $type
     * @return $this
     */
    public function setType($type): self
    {
        $this->type = $type;
        return $this;
    }

    /**
     * @param $value
     * @return $this
     */
    public function setValue($value): self
    {
        $this->value = $value;
        return $this;
    }

    /**
     * @param array $subColumns
     * @return $this
     */
    public function setSubColumns(array $subColumns): self
    {
        $this->subColumns = $subColumns;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getType()
    {
        return $this->type;
    }

    /**
     * @return mixed
     */
    public function getValue()
    {
        return $this->value;
    }

    /**
     * @return Column[]
     */
    public function getSubColumns(): array
    {
        return $this->subColumns;
    }
}
