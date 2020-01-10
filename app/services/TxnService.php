<?php

namespace App\services;

/**
 */
class TxnService extends \SwFwLess\services\GrpcUnaryService implements \App\services\TxnInterface
{

    /**
     * @param \Roykv\BeginRequest $request
     * @return \Roykv\BeginReply
     */
    public function Begin(\Roykv\BeginRequest $request)
    {
        //todo implements interface
    }
    /**
     * @param \Roykv\CommitRequest $request
     * @return \Roykv\CommitReply
     */
    public function Commit(\Roykv\CommitRequest $request)
    {
        //todo implements interface
    }
    /**
     * @param \Roykv\RollbackRequest $request
     * @return \Roykv\RollbackReply
     */
    public function Rollback(\Roykv\RollbackRequest $request)
    {
        //todo implements interface
    }

}
