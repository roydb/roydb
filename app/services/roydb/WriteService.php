<?php

namespace App\services\roydb;

use App\components\optimizers\CostBasedOptimizer;
use App\components\optimizers\RulesBasedOptimizer;
use App\components\Parser;
use App\components\plans\Plan;
use App\components\storage\roykv\Roykv;
use Roydb\DeleteResponse;
use Roydb\InsertResponse;

/**
 */
class WriteService extends \SwFwLess\services\GrpcUnaryService implements WriteInterface
{

    /**
     * @param \Roydb\InsertRequest $request
     * @return InsertResponse
     * @throws \Throwable
     */
    public function Insert(\Roydb\InsertRequest $request)
    {
        $sql = $request->getSql();
        $ast = Parser::fromSql($sql)->parseAst();
        //todo 数据库权限检查
        $plan = Plan::create($ast, new Roykv());
        $plan = RulesBasedOptimizer::fromPlan($plan)->optimize();
        $plan = CostBasedOptimizer::fromPlan($plan)->optimize();
        $affectedRows = $plan->execute();

        return (new InsertResponse())->setAffectedRows($affectedRows);
    }

    /**
     * @param \Roydb\DeleteRequest $request
     * @return DeleteResponse
     * @throws \Throwable
     */
    public function Delete(\Roydb\DeleteRequest $request)
    {
        $sql = $request->getSql();
        $ast = Parser::fromSql($sql)->parseAst();
        //todo 数据库权限检查
        $plan = Plan::create($ast, new Roykv());
        $plan = RulesBasedOptimizer::fromPlan($plan)->optimize();
        $plan = CostBasedOptimizer::fromPlan($plan)->optimize();
        $affectedRows = $plan->execute();

        return (new DeleteResponse())->setAffectedRows($affectedRows);
    }

    /**
     * @param \Roydb\UpdateRequest $request
     * @return \Roydb\UpdateResponse|void
     */
    public function Update(\Roydb\UpdateRequest $request)
    {
        // TODO: Implement Update() method.
    }

}
