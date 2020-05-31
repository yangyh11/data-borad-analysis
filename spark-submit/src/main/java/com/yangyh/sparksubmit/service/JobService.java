//package com.yangyh.sparksubmit.service;
//
//import com.yangyh.sparksubmit.entity.Job;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Service;
//
//import java.io.File;
//
///**
// * @description:
// * @author: yangyh
// * @create: 2020-05-31 21:11
// */
//@Service
//public class JobService {
//
//    private Logger logger = LoggerFactory.getLogger(this.getClass());
//
//
//    public Boolean start(Job job){
//        SparkAppHandle handler = null;
//        try {
//            handler = launch(job);
//        }catch (Exception e){
//            logger.error(e.getMessage());
//            return false;
//        }
////        job.setApp_id(handler.getAppId());
////        save(job);
//        return true;
//    }
//
//    private SparkAppHandle launch(Job job) throws Exception {
//        logger.warn("launch sparkLauncher");
//        SparkLauncher launcher = new SparkLauncher()
//                .setAppName(job.getName())
//                .setSparkHome(TeddyConf.get("spark.home"))
//                .setMaster(job.getMaster())
//                .setAppResource(TeddyConf.get("lib.home")+job.getApp_resource())
//                .setMainClass(job.getMain_class())
//                .addAppArgs(job.getArgs())
//                .setDeployMode(job.getDeploy_mode());
//
//        String settings = job.getConfig();
//        for(String setting : StringUtils.splitByWholeSeparator(settings,";")){
//            String[] strings = StringUtils.split(setting, "=");
//            launcher.setConf(strings[0],strings[1]);
//        }
//
//        launcher.redirectOutput(new File(TeddyConf.get("log.file")));
//        launcher.redirectError(new File(TeddyConf.get("log.file")));
//
//
//        logger.warn("构建："+ JSON.toJSONString(launcher));
//        SparkAppHandle handler = launcher.startApplication();
//
//        // 阻塞登到有id再返回
//        while(handler.getAppId()==null){
//
//            logger.warn("waiting for appId: "+handler.getAppId()+" "+handler.getState());
//
//            if(handler.getState().isFinal()){
//                throw new RuntimeException("handler "+handler.getState());
//            }
//
//            try {
//                Thread.sleep(2000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//        logger.warn("Get appId:"+handler.getAppId());
//        return handler;
//    }
//}
