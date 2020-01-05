<?php

namespace App\services;

/**
 */
interface KvInterface
{

    /**
     * @param \Roykv\GetRequest $request
     * @return \Roykv\GetReply
     */
    public function Get(\Roykv\GetRequest $request);
    /**
     * @param \Roykv\ExistRequest $request
     * @return \Roykv\ExistReply
     */
    public function Exist(\Roykv\ExistRequest $request);
    /**
     * @param \Roykv\ScanRequest $request
     * @return \Roykv\ScanReply
     */
    public function Scan(\Roykv\ScanRequest $request);
    /**
     * @param \Roykv\MGetRequest $request
     * @return \Roykv\MGetReply
     */
    public function MGet(\Roykv\MGetRequest $request);
    /**
     * @param \Roykv\GetAllRequest $request
     * @return \Roykv\GetAllReply
     */
    public function GetAll(\Roykv\GetAllRequest $request);

}
