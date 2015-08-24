<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"	
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
    xmlns:oai="http://www.openarchives.org/OAI/2.0/"  
    xmlns:marc="http://www.loc.gov/MARC21/slim"	
    xmlns="http://ands.org.au/standards/rif-cs/registryObjects"
    exclude-result-prefixes="xs xsi xsl oai marc">

    <!-- =========================================== -->
    <!-- Configuration                               -->
    <!-- =========================================== -->

    <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
    <xsl:param name="global_group" select="'CERN'"/>
    <!-- <xsl:param name="global_acronym" select="'Dryad'"/> -->
    <xsl:param name="originatingSource" select="'CERN'"/> 

    <!-- =========================================== -->
    <!-- Default template: copy everything           -->
    <!-- =========================================== -->

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>


    <!-- =========================================== -->
    <!-- RegistryObjects (root) Template             -->
    <!-- =========================================== -->
    
    <xsl:template match="oai:record">
	<!-- define recordKey variable -->	
	<xsl:variable name="recordKey" select="./oai:header/oai:identifier/text()"/>

	<!-- define recordType variable (can be multiple) -->	
        <xsl:variable name="recordType" select=".//marc:record/marc:datafield[@tag='980']/marc:subfield[@code='a']/text()" />

	<!-- test if recordType is ARTICLE or BOOK -->
	<xsl:if test="boolean($recordType = 'ARTICLE') or boolean($recordType = 'BOOK')">
	
	    <!-- create OAI Record object -->
	    <record>

		<!-- Copy OAI Header -->	
		<xsl:copy-of select="./oai:header"/>

		<!-- create OAI Metadaya object -->
	    	<metadata>	
	    	    <!-- create RIF:CS Registry objects collection -->
                    <registryObjects>
			<!-- create RIF:CS Registry objectss schema location -->
                        <xsl:attribute name="xsi:schemaLocation">
                            <xsl:text>http://ands.org.au/standards/rif-cs/registryObjects http://services.ands.org.au/documentation/rifcs/schema/registryObjects.xsd</xsl:text>
                        </xsl:attribute>
			
			<!-- create RIF:CS Registry object -->
		        <registryObject>
			    <!-- create RIF:CS Registry object group attribute -->
			    <xsl:attribute name="group">
				<xsl:value-of select="$global_group"/>
			    </xsl:attribute>

		    	    <!-- create RIF:CS Key object -->
		    	    <key>
		        	<xsl:copy-of select="$recordKey"/>
		    	    </key>

			    <!-- create RIF:CS Originating Source Object -->	
		    	    <originatingSource>
		       		<xsl:value-of select="$originatingSource"/>    
		    	    </originatingSource>
	
			    <!-- create RIF:CS Collection Object -->
		    	    <collection>
				<!-- create RIF:CS Collection type attribute -->
				<xsl:attribute name="type">
			   	    <xsl:text>publication</xsl:text>
		        	 </xsl:attribute>

				<!-- create Title object -->
				<xsl:apply-templates select=".//marc:record/marc:datafield[@tag='245']/marc:subfield[@code='a']/text()" mode="title"/>

				<!-- create Description object -->
				<!-- <xsl:apply-templates select=".//marc:record/marc:datafield[@tag='520']/marc:subfield[@code='a']/text()" mode="description"/> -->

				<!-- create Description object -->
				<xsl:apply-templates select=".//marc:record" mode="description"/>

				<!-- create Subject object -->
				<xsl:apply-templates select=".//marc:record/marc:datafield[@tag='653' and @ind1='1']/marc:subfield[@code='a']/text()" mode="subject"/>

				<!-- create Subject object -->
				<xsl:apply-templates select=".//marc:record/marc:datafield[@tag='650' and @ind1='1' and @ind2='7']/marc:subfield[@code='a']/text()" mode="subject"/>

				<!-- create Idenifier (DOI) object -->
				<xsl:apply-templates select=".//marc:record/marc:datafield[@tag='024' and @ind1='7']/marc:subfield[@code='a']/text()" mode="identifier_doi"/>
				
				<!-- create Email object -->
				<xsl:apply-templates select=".//marc:record/marc:datafield[@tag='856' and @ind1='0']" mode="email"/>

				<!-- create URL object -->
				<xsl:apply-templates select=".//marc:record/marc:datafield[@tag='035']" mode="url"/>

				<!-- create URL object -->
				<xsl:apply-templates select=".//marc:record/marc:datafield[@tag='856' and @ind1='4']" mode="url2"/>

			    </collection>	
    		        </registryObject>

                    </registryObjects>
                </metadata>
	    </record>
	    
        </xsl:if> 	
	
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/identifier(doi) template     -->
    <!-- =========================================== -->

    <xsl:template match="node()" mode="identifier_doi">
        <identifier>
	    <xsl:attribute name="type">
		<xsl:text>doi</xsl:text>
            </xsl:attribute>
            <xsl:value-of select="normalize-space(.)"/>
        </identifier>
    </xsl:template>


    <!-- =========================================== -->
    <!-- RegistryObject/location/address (url)       -->
    <!-- =========================================== -->

    <xsl:template match="marc:datafield" mode="url">
	<xsl:variable name="type" select="./marc:subfield[@code='9']/text()" />	
	<xsl:variable name="id" select="normalize-space(./marc:subfield[@code='a']/text())" />	
	<xsl:if test="$type='Inspire' and $id != ''">	
	    <location>
	        <address>
		    <electronic>
		        <xsl:attribute name="type">
			    <xsl:text>url</xsl:text>
            	        </xsl:attribute>
		        <value>	
			    <xsl:text>http://inspirehep.net/record/</xsl:text>  
		     	    <xsl:value-of select="$id"/>
		        </value>
		    </electronic>
	        </address>
	    </location>
	</xsl:if>
    </xsl:template>	

    <!-- =========================================== -->
    <!-- RegistryObject/location/address (url2)      -->
    <!-- =========================================== -->

    <xsl:template match="marc:datafield" mode="url2">
	<xsl:variable name="url" select="normalize-space(./marc:subfield[@code='u']/text())" />	
	<xsl:variable name="type" select="./marc:subfield[@code='y']/text()" />	
	<xsl:variable name="subtype" select="./marc:subfield[@code='x']/text()" />	
	<xsl:if test="$url != ''">	
	    <location>
	        <address>
		    <electronic>
		        <xsl:attribute name="type">
			    <xsl:text>url</xsl:text>
            	        </xsl:attribute>
		        <value>	
			    <xsl:text>http://inspirehep.net/record/</xsl:text>  
		     	    <xsl:value-of select="$url"/>
		        </value>
			<xsl:if test="$type != ''">
			    <title>
				<xsl:value-of select="$type"/>	
				<xsl:if test="$subtype != ''">		
				    <xsl:value-of select="concat(' (', $subtype, ')')"/>
				</xsl:if>
     			    </title>
			</xsl:if>			
		    </electronic>
	        </address>
	    </location>
	</xsl:if>
    </xsl:template>


    <!-- =========================================== -->
    <!-- RegistryObject/location/address (email)     -->
    <!-- =========================================== -->

    <xsl:template match="marc:datafield" mode="url2">
	<xsl:variable name="email" select="normalize-space(./marc:subfield[@code='f']/text())" />	
	<xsl:if test="$email != ''">	
	    <location>
	        <address>
		    <electronic>
		        <xsl:attribute name="type">
			    <xsl:text>email</xsl:text>
            	        </xsl:attribute>
		        <value>	
		     	    <xsl:value-of select="$email"/>
		        </value>
		    </electronic>
	        </address>
	    </location>
	</xsl:if>
    </xsl:template>			

    <!-- =========================================== -->
    <!-- RegistryObject/name/namePart(primary) -->
    <!-- =========================================== -->

    <xsl:template match="node()" mode="title">
	<name>
	    <namePart>
	    	<xsl:attribute name="type">
		    <xsl:text>primary</xsl:text>
            	</xsl:attribute>
	    	<xsl:value-of select="normalize-space(.)"/>
	    </namePart>	
	</name>
    </xsl:template>

    <!-- =========================================== -->
    <!-- RegistryObject/description template         -->
    <!-- =========================================== -->

    <!-- <xsl:template match="node()" mode="description">
	<description>
	    <xsl:attribute name="type">
		<xsl:text>full</xsl:text>
            </xsl:attribute>
	    <xsl:value-of select="normalize-space(.)"/>	
        </description>
    </xsl:template> -->

    <!-- =========================================== -->
    <!-- RegistryObject/description template         -->
    <!-- =========================================== -->

    <xsl:template match="marc:record" mode="description">
	<xsl:variable name="abstract" select="./marc:datafield[@tag='520']/marc:subfield[@code='a']/text()"/>
	<xsl:variable name="authors" select="./marc:datafield[@tag='100' or @tag='700']/marc:subfield[@code='a']/text()"/>
	<xsl:if test="$abstract or $authors"> 
	    <description>
	        <xsl:attribute name="type">
		    <xsl:text>full</xsl:text>
                </xsl:attribute>
		<xsl:if test="$abstract">
		    <xsl:value-of select="$abstract"/>
		    <xsl:if test="$authors">
			<xsl:text>&#xa;</xsl:text>
		    </xsl:if>
		</xsl:if>
		<xsl:if test="$authors">
	            <xsl:for-each select="$authors">
		        <xsl:text>Author: </xsl:text>
		        <xsl:value-of select="."/>	
		        <xsl:if test="position() &lt; last()">
			    <xsl:text>&#xa;</xsl:text>
			</xsl:if>
	            </xsl:for-each>
		</xsl:if>
            </description>	
	</xsl:if>	
    </xsl:template> 

    <!-- =========================================== -->
    <!-- RegistryObject/subject                     -->
    <!-- =========================================== -->

    <xsl:template match="node()" mode="subject">
	<subject>
	    <xsl:attribute name="type">
		<xsl:text>local</xsl:text>
            </xsl:attribute>	
	    <xsl:value-of select="normalize-space(.)"/>
	</subject>	
    </xsl:template>	
</xsl:stylesheet>

