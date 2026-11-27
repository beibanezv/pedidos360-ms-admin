package com.pedidos360.admin.kafka;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
class KafkaAdminConfig {
  @Bean(destroyMethod = "close")
  Admin kafkaAdmin(@Value("${p360.kafka.bootstrap-servers}") String bootstrap) {
    return Admin.create(Map.of(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2500,
        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 4500));
  }

  @Bean
  SecurityFilterChain kafkaSecurity(HttpSecurity http, JwtDecoder decoder) throws Exception {
    var authorities = new JwtGrantedAuthoritiesConverter();
    authorities.setAuthoritiesClaimName("roles");
    authorities.setAuthorityPrefix("ROLE_");
    var converter = new JwtAuthenticationConverter();
    converter.setJwtGrantedAuthoritiesConverter(authorities);
    // Un solo matcher para ambas APIs de administración: antes Rabbit quedaba
    // fuera del filtro y cualquier token (o ninguno) llegaba al controller.
    return http.securityMatcher("/api/kafka/**", "/api/rabbitmq/**")
        .csrf(csrf -> csrf.disable())
        .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(auth -> auth.anyRequest().hasRole("Admin"))
        .oauth2ResourceServer(oauth -> oauth.jwt(jwt -> jwt.decoder(decoder)
            .jwtAuthenticationConverter(converter)))
        .build();
  }

  @Bean
  JwtDecoder kafkaJwtDecoder(@Value("${p360.azure.tenant-id}") String tenant,
      @Value("${p360.azure.audience}") String audience) {
    var decoder = NimbusJwtDecoder.withJwkSetUri(
        "https://login.microsoftonline.com/" + tenant + "/discovery/keys").build();
    decoder.setJwtValidator(new DelegatingOAuth2TokenValidator<>(
        JwtValidators.createDefaultWithIssuer("https://sts.windows.net/" + tenant + "/"),
        new JwtClaimValidator<List<String>>("aud", aud -> aud != null && aud.contains(audience))));
    return decoder;
  }
}
