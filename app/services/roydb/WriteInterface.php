<?php

namespace App\services\roydb;

/**
 */
interface WriteInterface
{

    /**
     * @param \Roydb\InsertRequest $request
     * @return \Roydb\InsertResponse
     */
    public function Insert(\Roydb\InsertRequest $request);

}
