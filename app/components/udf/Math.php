<?php

namespace App\components\udf;

use App\components\elements\Aggregation;
use App\components\elements\Column;
use App\services\roydb\MathClient;
use Roydbudf\SinRequest;

class Math
{
    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return float|null
     * @throws \Exception
     */
    public static function sin($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];
        if ($column->getType() === 'const') {
            $degree = $column->getValue();
        } else {
            if ($column->getValue() === '*') {
                throw new \Exception('Unsupported column named as \'*\' passed to sin function');
            }

            if ($row instanceof Aggregation) {
                $degree = $row->getFirstRow()[$column->getValue()];
            } else {
                $degree = $row[$column->getValue()];
            }
        }

        return sin($degree);
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return float
     * @throws \Exception
     */
    public static function cos($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];
        if ($column->getType() === 'const') {
            return cos($column->getValue());
        } else {
            if ($column->getValue() === '*') {
                throw new \Exception('Unsupported column named as \'*\' passed to cos function');
            }

            if ($row instanceof Aggregation) {
                $degree = $row->getFirstRow()[$column->getValue()];
            } else {
                $degree = $row[$column->getValue()];
            }
            return cos($degree);
        }
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return float
     * @throws \Exception
     */
    public static function sqrt($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];
        if ($column->getType() === 'const') {
            return sqrt($column->getValue());
        } else {
            if ($column->getValue() === '*') {
                throw new \Exception('Unsupported column named as \'*\' passed to sqrt function');
            }

            if ($row instanceof Aggregation) {
                $number = $row->getFirstRow()[$column->getValue()];
            } else {
                $number = $row[$column->getValue()];
            }
            return sqrt($number);
        }
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return float|int
     * @throws \Exception
     */
    public static function pow($parameters, $row, $resultSet)
    {
        /** @var Column $baseColumn */
        $baseColumn = $parameters[0];
        if ($baseColumn->getType() === 'const') {
            $base = $baseColumn->getValue();
        } else {
            if ($baseColumn->getValue() === '*') {
                throw new \Exception('Unsupported column named as \'*\' first passed to sqrt function');
            }

            if ($row instanceof Aggregation) {
                $base = $row->getFirstRow()[$baseColumn->getValue()];
            } else {
                $base = $row[$baseColumn->getValue()];
            }
        }

        /** @var Column $expColumn */
        $expColumn = $parameters[1];
        if ($expColumn->getType() === 'const') {
            $exp = $expColumn->getValue();
        } else {
            if ($expColumn->getValue() === '*') {
                throw new \Exception('Unsupported column named as \'*\' second passed to sqrt function');
            }

            if ($row instanceof Aggregation) {
                $exp = $row->getFirstRow()[$expColumn->getValue()];
            } else {
                $exp = $row[$expColumn->getValue()];
            }
        }

        return pow($base, $exp);
    }
}
