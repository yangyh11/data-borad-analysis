package com.yangyh.sparksubmit.service;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2020-05-31 21:31
 */
@Service
public class SubmitService {

    public void submitJob() throws InterruptedException {
        // 待提交给spark集群处理的spark application jar（即appJar）所在路径
        String appJarName = "/root/offline-job-babel-1.0-SNAPSHOT.jar";
        SparkLauncher launcher = new SparkLauncher();
        launcher.setAppResource(appJarName);
// 设置spark driver主类，即appJar的主类
        launcher.setMainClass("com.offline.job.babel.log.LogParseEngine");
// 添加传递给spark driver mian方法的参数
        launcher.addAppArgs("arg1", "arg2", "arg3" );
// 设置该spark application的master
        launcher.setMaster("yarn"); // 在yarn-cluster上启动，也可以再local[*]上
        launcher.setDeployMode("cluster");
//        launcher.setMaster("local");
        launcher.setSparkHome("/opt/spark-2.3.1");
// 关闭sparksubmit的详细报告
//        launcher.setVerbose(false);
// 设置用于执行appJar的spark集群分配的driver、executor内存等参数
//        launcher.setConf(SparkLauncher.DRIVER_MEMORY, "2g");
//        launcher.setConf(SparkLauncher.EXECUTOR_MEMORY, "1g");
//        launcher.setConf(SparkLauncher.EXECUTOR_CORES, "16");
//        launcher.setConf("spark.default.parallelism", "128");
//        launcher.setConf("spark.executor.instances", "16");

// 启动执行该application
        SparkAppHandle handle = null;
        try {
            handle = launcher.startApplication();
        } catch (IOException e) {
            e.printStackTrace();
        }

// application执行失败重试机制
// 最大重试次数
        boolean failedflag = false;
        int maxRetrytimes = 3;
        int currentRetrytimes = 0;
        while (handle.getState() != SparkAppHandle.State.FINISHED) {
            currentRetrytimes++;
            // 每6s查看application的状态（UNKNOWN、SUBMITTED、RUNNING、FINISHED、FAILED、KILLED、 LOST）
            Thread.sleep(6000L);
            System.out.println("applicationId is: " + handle.getAppId());
            System.out.println("current state: " + handle.getState());
            if ((handle.getAppId() == null && handle.getState() == SparkAppHandle.State.FAILED) && currentRetrytimes > maxRetrytimes) {
                System.out.println(String.format("tried launching application for %s times but failed, exit.", maxRetrytimes));
                failedflag = true;
                break;
            }

        }
    }
}
