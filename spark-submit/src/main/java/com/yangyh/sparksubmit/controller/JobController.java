package com.yangyh.sparksubmit.controller;

import com.yangyh.sparksubmit.service.SubmitService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * @description:
 * @author: yangyh
 * @create: 2020-05-31 21:09
 */
@RestController
@RequestMapping("job")
public class JobController {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private SubmitService submitService;

    @RequestMapping(value = "submit", method = RequestMethod.POST)
    public String submit() throws InterruptedException {
        submitService.submitJob();
        return "提交成功";
    }
}
