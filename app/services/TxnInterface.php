<?php

namespace App\services;

/**
 */
interface TxnInterface
{

    /**
     * @param \Roykv\BeginRequest $request
     * @return \Roykv\BeginReply
     */
    public function Begin(\Roykv\BeginRequest $request);
    /**
     * @param \Roykv\CommitRequest $request
     * @return \Roykv\CommitReply
     */
    public function Commit(\Roykv\CommitRequest $request);
    /**
     * @param \Roykv\RollbackRequest $request
     * @return \Roykv\RollbackReply
     */
    public function Rollback(\Roykv\RollbackRequest $request);

}
