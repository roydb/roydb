<?php

namespace App\components\udf;

use App\components\elements\Column;

class Aggregate
{
    public static function count($parameters, $resultSet)
    {
        /** @var Column $column */
        $column = $parameters[0];

        $columnType = $column->getType();
        $columnValue = $column->getValue();
        if ($columnType === 'const') {
            return count($resultSet);
        } else {
            if ($columnValue === '*') {
                return count($resultSet);
            } else {
                $count = 0;
                foreach ($resultSet as $i => $row) {
                    if (!is_null($row[$columnValue])) {
                        ++$count;
                    }
                }
                return $count;
            }
        }
    }
}
