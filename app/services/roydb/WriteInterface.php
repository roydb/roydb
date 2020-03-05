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
    /**
     * @param \Roydb\DeleteRequest $request
     * @return \Roydb\DeleteResponse
     */
    public function Delete(\Roydb\DeleteRequest $request);

}
