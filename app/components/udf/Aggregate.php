<?php

namespace App\components\udf;

use App\components\elements\Aggregation;
use App\components\elements\Column;
use SwFwLess\facades\File;

class Aggregate
{
    public static function count($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();

        if ($row instanceof Aggregation) {
            $resultSet = $row->getRows();
        }

        if ($columnType === 'const') {
            return count($resultSet);
        } else {
            if ($columnValue === '*') {
                return count($resultSet);
            } else {
                return count(array_filter(array_column($resultSet, $columnValue), function ($value) {
                    return !is_null($value);
                }));
            }
        }
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return mixed
     * @throws \Exception
     */
    public static function max($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            throw new \Exception('Unsupported const param passed to max function');
        } else {
            if ($columnValue === '*') {
                throw new \Exception('Unsupported column named as \'*\' passed to max function');
            }

            if ($row instanceof Aggregation) {
                $values = array_column($row->getRows(), $columnValue);
            } else {
                $values = array_column($resultSet, $columnValue);
            }

            if (is_float($values[0]) || is_double($values[0]) || is_integer($values[0])) {
                $numbersCount = count($values);
                $udf = \FFI::cdef("double ArrayMax(double numbers[], int size);", File::basePath() . 'libs/udf/libcudf.so');
                $arr = \FFI::new('double[' . ((string)$numbersCount) . ']');
                for ($i = 0; $i < $numbersCount; ++$i) {
                    $arr[$i] = $values[$i];
                }
                return $udf->ArrayMax($arr, $numbersCount);
            } else {
                return max($values);
            }
        }
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return mixed
     * @throws \Exception
     */
    public static function min($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            throw new \Exception('Unsupported const param passed to min function');
        } else {
            if ($columnValue === '*') {
                throw new \Exception('Unsupported column named as \'*\' passed to min function');
            }

            if ($row instanceof Aggregation) {
                $values = array_column($row->getRows(), $columnValue);
            } else {
                $values = array_column($resultSet, $columnValue);
            }

            if (is_float($values[0]) || is_double($values[0]) || is_integer($values[0])) {
                $numbersCount = count($values);
                $udf = \FFI::cdef("double ArrayMin(double numbers[], int size);", File::basePath() . 'libs/udf/libcudf.so');
                $arr = \FFI::new('double[' . ((string)$numbersCount) . ']');
                for ($i = 0; $i < $numbersCount; ++$i) {
                    $arr[$i] = $values[$i];
                }
                return $udf->ArrayMin($arr, $numbersCount);
            } else {
                return min($values);
            }
        }
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return mixed
     * @throws \Exception
     */
    public static function sum($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            throw new \Exception('Unsupported const param passed to sum function');
        } else {
            if ($columnValue === '*') {
                throw new \Exception('Unsupported column named as \'*\' passed to sum function');
            }

            if ($row instanceof Aggregation) {
                $numbers = array_column($row->getRows(), $columnValue);
            } else {
                $numbers = array_column($resultSet, $columnValue);
            }

            $numbersCount = count($numbers);
            $udf = \FFI::cdef("double ArraySum(double numbers[], int size);", File::basePath() . 'libs/udf/libcudf.so');
            $arr = \FFI::new('double[' . ((string)$numbersCount) . ']');
            for ($i = 0; $i < $numbersCount; ++$i) {
                $arr[$i] = $numbers[$i];
            }

            return $udf->ArraySum($arr, $numbersCount);
        }
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return mixed
     * @throws \Exception
     */
    public static function avg($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            throw new \Exception('Unsupported const param passed to avg function');
        } else {
            if ($columnValue === '*') {
                throw new \Exception('Unsupported column named as \'*\' passed to avg function');
            }

            if ($row instanceof Aggregation) {
                $numbers = array_column($row->getRows(), $columnValue);
            } else {
                $numbers = array_column($resultSet, $columnValue);
            }

            $numbersCount = count($numbers);
            $udf = \FFI::cdef("double ArrayAvg(double numbers[], int size);", File::basePath() . 'libs/udf/libcudf.so');
            $arr = \FFI::new('double[' . ((string)$numbersCount) . ']');
            for ($i = 0; $i < $numbersCount; ++$i) {
                $arr[$i] = $numbers[$i];
            }

            return $udf->ArrayAvg($arr, $numbersCount);
        }
    }

    /**
     * @param $parameters
     * @param $row
     * @param $resultSet
     * @return mixed
     * @throws \Exception
     */
    public static function first($parameters, $row, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            throw new \Exception('Unsupported const param passed to first function');
        } else {
            if ($columnValue === '*') {
                if ($row instanceof Aggregation) {
                    return $row->getFirstRow();
                } else {
                    return $resultSet[0];
                }
            }
            if ($row instanceof Aggregation) {
                return $row->getFirstRow()[$columnValue];
            } else {
                return $resultSet[0][$columnValue];
            }
        }
    }
}
