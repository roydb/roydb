<?php

namespace App\services\roydb;

/**
 */
interface QueryInterface
{

    /**
     * @param \Roydb\SelectRequest $request
     * @return \Roydb\SelectResponse
     */
    public function Select(\Roydb\SelectRequest $request);

}
