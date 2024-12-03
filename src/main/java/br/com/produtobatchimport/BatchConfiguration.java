package br.com.produtobatchimport;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class BatchConfiguration {

    private final JobRepository jobRepository;

    public BatchConfiguration(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Bean
    public Job processarProdutos(JobRepository jobRepository, Step step){
        return new JobBuilder("processarProdutos", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(step)
                .build();
    }

    @Bean
    public Step processarProdutosStep(JobRepository jobRepository,
                                      PlatformTransactionManager transactionManager,
                                      ItemReader<Produtos> itemReader,
                                      ItemWriter<Produtos> itemWriter){
        return new StepBuilder("step", jobRepository)
                .<Produtos, Produtos>chunk(32, transactionManager)
                .reader(itemReader)
                .writer(itemWriter)
                .build();
    }

    @Bean
    public ItemReader<Produtos> itemReader(){
        BeanWrapperFieldSetMapper<Produtos> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(Produtos.class);
        return new FlatFileItemReaderBuilder<Produtos>()
                .name("produtosItemReader")
                .resource(new ClassPathResource("produtos.csv"))
                .delimited()
                .names("nome", "descricao", "preco", "tamanho", "cor", "quantidade")
                .fieldSetMapper(mapper)
                .build();
    }

    @Bean
    public ItemWriter<Produtos> itemWriter(HikariDataSource dataSource){
        return new JdbcBatchItemWriterBuilder<Produtos>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .dataSource(dataSource)
                .sql("INSERT INTO produtos \n" +
                        "(descricao, nome, preco, quantidade, cor, tamanho) \n" +
                        "VALUES(:descricao, :nome, :preco, :quantidade, :cor, :tamanho);")
                .build();
    }


}
