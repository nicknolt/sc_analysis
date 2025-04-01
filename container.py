from typing import Tuple

from dependency_injector import providers, containers

from cache_repository import CacheRepository
from data_service import DataService
from lmt.lmt_service import LMTService
from lmt.lmt_video_service import LMTVideoService
from parameters import Parameters



class Container(containers.DeclarativeContainer):

    config = providers.Configuration()

    wiring_config = containers.WiringConfiguration(
        modules=[
            "process",
            "lmt.lmt2batch_link_process"
        ],
        packages=[
            "batch_process"
        ]
    )


    cache_repository = providers.Singleton(
        CacheRepository,
        result_dir=config.general.result_dir.required()
    )


    data_service = providers.Singleton(
        DataService,
        data_dir=config.general.data_dir.required()
    )

    lmt_service = providers.Singleton(
        LMTService,
        lmt_dir=config.general.lmt_dir.required()
    )

    video_service = providers.Singleton(
        LMTVideoService,
        video_dir=config.general.video_dir.required()
    )

    parameters = providers.Singleton(
        Parameters,
        max_sequence_duration=config.process_parameters.max_sequence_duration.required().as_int(),
        lever_loc=config.process_parameters.lever_loc.required(),
        feeder_loc=config.process_parameters.feeder_loc.required()
    )

    # data_service = providers.Singleton(DataService,
    #                                    data_dir=config.general.data_dir.required())
    #
    # tracking_repo = providers.Singleton(TrackingRepository,
    #                                     data_dir=config.general.data_dir.required())
    #
    # # time_drift_estimator = providers.Factory(TimeDriftEstimator,
    # #                                             data_service=data_service,
    # #                                          )
    # #
    # # time_drift_correction = providers.Singleton(TimeDriftCorrection,
    # #                                             left_estimator=time_drift_estimator('L')
    # #                                             )
    # param_provider = providers.Singleton(
    #     ParamProvider,
    #     data_service=data_service
    # )