'''
    HLO Data Aggregator
    a) Receive Redapanda message ('fe2data' topic) from HLO_FE_EP, according to protobuf 'message HLOFEInput' 
    b) Query CB for Service data
    c) Format query with minimum IE requirements for CB
    d) Query CB for candidate IEs
    e) Create message for HLO_Allocator, according to protobuf 'message HLOAllocatorOutput'
    f) Produce message to redpanda ('data2allocator' topic) to HLO_Allocator

    For developing use the following to access redpanDa (VPN is required first)
    kubectl port-forward -n redpanda svc/redpanda 9093:9093
    And add a record in /etc/hosts:
        127.0.0.1  redpanda-0.redpanda.redpanda.svc.cluster.local

'''
from app import app
from app.utils.log import get_app_logger


logger = get_app_logger()
logger.info('** IE Resource Alert EP started **')
logger.info("test with flux")




if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8081)
