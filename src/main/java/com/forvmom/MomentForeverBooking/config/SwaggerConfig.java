package com.forvmom.MomentForeverBooking.config;

import io.swagger.v3.oas.models.servers.Server;
import org.springdoc.core.models.GroupedOpenApi;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

@Configuration
public class SwaggerConfig {

    private static final String SECURITY_SCHEME_NAME = "bearerAuth";

    // ✅ Group Booking APIs (Optional but Clean)
    @Bean
    public GroupedOpenApi bookingApi() {
        return GroupedOpenApi.builder()
                .group("1-booking")
                .pathsToMatch("/**")
                .displayName("Booking API")
                .build();
    }

    // ✅ Very Important – Add Context Path Here
    @Bean
    public OpenAPI customOpenAPI() {
        return new OpenAPI()
                .info(apiInfo())

                // 👇 THIS MUST MATCH YOUR CONTEXT PATH
                .addServersItem(new Server().url("/api/booking"))

                .addSecurityItem(new SecurityRequirement().addList(SECURITY_SCHEME_NAME))
                .components(new Components()
                        .addSecuritySchemes(SECURITY_SCHEME_NAME, createSecurityScheme()));
    }

    @Bean
    public OpenApiCustomizer publicEndpointsNoAuthCustomizer() {
        return openApi -> {
            if (openApi.getPaths() == null) {
                return;
            }
            openApi.getPaths().forEach((path, pathItem) -> {
                if (!path.startsWith("/public/") && !path.startsWith("/auth/")) {
                    return;
                }
                for (Operation operation : pathItem.readOperations()) {
                    operation.setSecurity(Collections.emptyList());
                }
            });
        };
    }

    private SecurityScheme createSecurityScheme() {
        return new SecurityScheme()
                .name(SECURITY_SCHEME_NAME)
                .type(SecurityScheme.Type.HTTP)
                .scheme("bearer")
                .bearerFormat("JWT")
                .in(SecurityScheme.In.HEADER)
                .description("Enter JWT token (without 'Bearer' prefix)");
    }

    private Info apiInfo() {
        return new Info()
                .title("MomentForever Booking API")
                .description("REST API documentation for Booking Service")
                .version("1.0.0")
                .contact(new Contact()
                        .name("API Support")
                        .email("support@forvmom.com"))
                .license(new License()
                        .name("Apache 2.0")
                        .url("https://www.apache.org/licenses/LICENSE-2.0"));
    }
}