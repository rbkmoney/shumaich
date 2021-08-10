package com.rbkmoney.shumaich;


import com.rbkmoney.shumaich.config.RocksDbConfiguration;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@RunWith(SpringRunner.class)
@ContextConfiguration(initializers = RocksdbTestBase.Initializer.class, classes = RocksDbConfiguration.class)
public abstract class RocksdbTestBase {

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    public static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
        @SneakyThrows
        @Override
        public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
            TestPropertyValues
                    .of("rocksdb.name=test",
                            "rocksdb.dir=" + folder.newFolder())
                    .applyTo(configurableApplicationContext.getEnvironment());
        }
    }

}
