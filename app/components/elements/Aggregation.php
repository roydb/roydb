<?php

namespace App\components\elements;

class Aggregation
{
    protected array $dimension = [];

    protected array $rows = [];

    protected array $aggregatedRow = [];

    /**
     * @param array $dimension
     * @return $this
     */
    public function setDimension(array $dimension): self
    {
        $this->dimension = $dimension;
        return $this;
    }

    /**
     * @param $dimension
     * @return $this
     */
    public function addDimension($dimension): self
    {
        $this->dimension[] = $dimension;
        return $this;
    }

    /**
     * @param array $rows
     * @return $this
     */
    public function setRows(array $rows): self
    {
        $this->rows = $rows;
        return $this;
    }

    /**
     * @param array $aggregatedRow
     * @return $this
     */
    public function setAggregatedRow(array $aggregatedRow): self
    {
        $this->aggregatedRow = $aggregatedRow;
        return $this;
    }

    /**
     * @param array $aggregatedRow
     * @return $this
     */
    public function mergeAggregatedRow(array $aggregatedRow): self
    {
        $this->aggregatedRow = $this->aggregatedRow + $aggregatedRow;
        return $this;
    }

    /**
     * @param $key
     * @param $value
     * @return $this
     */
    public function setOneAggregatedRow($key, $value): self
    {
        $this->aggregatedRow[$key] = $value;
        return $this;
    }

    /**
     * @return array
     */
    public function getDimension(): array
    {
        return $this->dimension;
    }

    /**
     * @return array
     */
    public function getRows(): array
    {
        return $this->rows;
    }

    /**
     * @return mixed
     */
    public function getFirstRow()
    {
        return $this->rows[0];
    }

    /**
     * @return mixed
     */
    public function getLastRow()
    {
        $rows = $this->rows;
        return end($rows);
    }

    /**
     * @return array
     */
    public function getAggregatedRow(): array
    {
        return $this->aggregatedRow;
    }
}
