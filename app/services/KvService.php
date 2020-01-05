<?php

namespace App\services;

/**
 */
class KvService extends \SwFwLess\services\GrpcUnaryService implements \App\services\KvInterface
{

    /**
     * @param \Roykv\GetRequest $request
     * @return \Roykv\GetReply
     */
    public function Get(\Roykv\GetRequest $request)
    {
        //todo implements interface
    }
    /**
     * @param \Roykv\ExistRequest $request
     * @return \Roykv\ExistReply
     */
    public function Exist(\Roykv\ExistRequest $request)
    {
        //todo implements interface
    }
    /**
     * @param \Roykv\ScanRequest $request
     * @return \Roykv\ScanReply
     */
    public function Scan(\Roykv\ScanRequest $request)
    {
        //todo implements interface
    }
    /**
     * @param \Roykv\MGetRequest $request
     * @return \Roykv\MGetReply
     */
    public function MGet(\Roykv\MGetRequest $request)
    {
        //todo implements interface
    }
    /**
     * @param \Roykv\GetAllRequest $request
     * @return \Roykv\GetAllReply
     */
    public function GetAll(\Roykv\GetAllRequest $request)
    {
        //todo implements interface
    }

}
