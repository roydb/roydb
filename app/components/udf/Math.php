<?php

namespace App\components\udf;

class Math
{
    public static function sin($parameters, $resultSet)
    {
        return sin($parameters[0]);
    }

    public static function cos($parameters, $resultSet)
    {
        return cos($parameters[0]);
    }

    public static function sqrt($parameters, $resultSet)
    {
        return sqrt($parameters[0]);
    }

    public static function pow($parameters, $resultSet)
    {
        return pow($parameters[0], $parameters[1]);
    }
}
