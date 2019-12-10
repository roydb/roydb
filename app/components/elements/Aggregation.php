<?php

namespace App\components\elements;

class Aggregation
{
    protected $dimension = [];

    protected $items = [];

    protected $aggregatedResult = [];

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
     * @param array $items
     * @return $this
     */
    public function setItems(array $items): self
    {
        $this->items = $items;
        return $this;
    }

    /**
     * @param array $aggregatedResult
     */
    public function setAggregatedResult(array $aggregatedResult): void
    {
        $this->aggregatedResult = $aggregatedResult;
    }

    /**
     * @param $key
     * @param $value
     * @return $this
     */
    public function setOneAggregatedResult($key, $value): self
    {
        $this->aggregatedResult[$key] = $value;
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
    public function getItems(): array
    {
        return $this->items;
    }

    /**
     * @return mixed
     */
    public function getFirstItem()
    {
        return $this->items[0];
    }

    /**
     * @return array
     */
    public function getAggregatedResult(): array
    {
        return $this->aggregatedResult;
    }
}
