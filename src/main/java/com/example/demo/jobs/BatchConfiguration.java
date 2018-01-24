package com.example.demo.jobs;

import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import com.example.demo.listener.JobCompletionNotificationListener;
import com.example.demo.model.Person;
import com.example.demo.processor.PersonItemProcessor;
import com.example.demo.processor.PersonItemProcessorAnother;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;
	@Autowired
	private DataSource dataSource;
	
	// start::readerwriterprocessor[]
	@Bean
	public FlatFileItemReader<Person> reader(){
		FlatFileItemReader<Person> reader = new FlatFileItemReader<>();
		reader.setResource(new ClassPathResource("person.csv"));
		reader.setLineMapper(new DefaultLineMapper<Person>() {{
		      setLineTokenizer(new DelimitedLineTokenizer() {{
		        setNames(new String[]{"firstName", "lastName"});
		      }});
		      setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
		        setTargetType(Person.class);
		      }});
		    }});
		
		return reader;
	}
	
	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}
	
	@Bean
	public JdbcBatchItemWriter<Person> writer(){
		JdbcBatchItemWriter<Person> writer = new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		writer.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
		writer.setDataSource(dataSource);
		return writer;
	}
	// end::readerwriterprocessor[]

	//Start of step2::readerWriterProcessor
	private String QUERY_FIND_PERSONS =
            "SELECT first_name, last_name FROM people ORDER BY first_name ASC";
	@Bean
	public JdbcCursorItemReader<Person> mysqlReader(){
		JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setSql(QUERY_FIND_PERSONS);
		reader.setRowMapper(new BeanPropertyRowMapper<>(Person.class));
		
		return reader;
	}
	
	@Bean
	public PersonItemProcessorAnother processorAnother() {
		return new PersonItemProcessorAnother();
	}
	
	@Bean
	public FlatFileItemWriter<Person> csvWriter(){
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<>();
		 writer.setResource(new FileSystemResource ("csv/output/output.csv"));
		 writer.setShouldDeleteIfExists(true);
		 
		 DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<Person>();
		 lineAggregator.setDelimiter(","); 
		 
		 BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<Person>();
		 String[] names = {"firstName", "lastName"};
		 fieldExtractor.setNames(names);
		 
		 lineAggregator.setFieldExtractor(fieldExtractor);
		 writer.setLineAggregator(lineAggregator);
		return writer;
	}
	//End of step2::readerWriterProcessor
	
	// tag::jobstep[]
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.start(step1())
				.next(step2())
				//.end()
				.build();
	}
	
	@Bean
	public Step step1(){// Jobs are built from steps, where each step can involve a reader, a processor, and a writer.
		return stepBuilderFactory.get("step1")
				.<Person, Person>chunk(10)//In the step definition, you define how much data to write at a time. In this case, it writes up to ten records at a time.
				.reader(reader())
				.processor(processor())
				.writer(writer())
				.build();
	}
	

	@Bean
	public Step step2() {
		// TODO Auto-generated method stub
		return stepBuilderFactory.get("step2")
				.<Person, Person>chunk(10)//In the step definition, you define how much data to write at a time. In this case, it writes up to ten records at a time.
				.reader(mysqlReader())
				.processor(processorAnother())
				.writer(csvWriter())
				.build();
	}
	
	/*@Bean
    protected Step step3(Tasklet tasklet) {
        return stepBuilderFactory.get("step3")
            .tasklet(tasklet)
            .build();
    }*/

	// end::jobstep[]
}
