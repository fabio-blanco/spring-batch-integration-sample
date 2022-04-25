package br.com.globalbyte.samples.springbatchintegrationsample;

import br.com.globalbyte.samples.springbatchintegrationsample.domain.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

/**
 * The processor that converts a person to an upper-case names person.
 */
public class PersonItemProcessor implements ItemProcessor<Person, Person> {
    private static final Logger log = LoggerFactory.getLogger(PersonItemProcessor.class);

    /**
     * Converts the names of a person to upper-case names
     *
     * @param person to be processed, never {@code null}.
     * @return A person with both first and last names upper-cased
     * @throws Exception thrown if exception occurs during processing.
     */
    @Override
    public Person process(final Person person) throws Exception {
        final String firstName = person.getFirstName().toUpperCase();
        final String lastName = person.getLastName().toUpperCase();

        final Person transformedPerson = new Person(firstName, lastName);

        log.info("Converting (" + person + ") into (" + transformedPerson + ")");

        return transformedPerson;
    }
}
